//! Db-level tests of the index blob cache (`index_blob_cache`): admission on
//! the syscall path only, single-flight fills across concurrent lookups,
//! explicit drops when a flush replaces a cached blob, and differential
//! runs of one workload against a cached and an uncached database.
use super::super::*;
use super::{
    cell_key, lookup_counts, sixteen_cells, unmapped_window_config, write_blobs_outside_window,
};
use crate::config::Config;
use crate::context::ReadType;
use crate::failpoints::FailPoint;
use crate::key_shape::{KeyShapeBuilder, KeySpace, KeySpaceConfig, KeyType};
use crate::latch::Latch;
use crate::metrics::Metrics;
use crate::relocation::{RelocationStrategy, compute_target_position_from_ratio};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Enables the cache with `budget` bytes and admits every syscall-path blob.
fn with_cache(mut config: Config, budget: usize) -> Arc<Config> {
    config.index_blob_cache_bytes = Some(budget);
    config.index_blob_cache_min_bytes = 1;
    Arc::new(config)
}

/// Index blobs of `ks` loaded through `Db::load` with `read_type`.
fn index_loads(metrics: &Metrics, db: &Db, ks: KeySpace, read_type: ReadType) -> u64 {
    metrics
        .read
        .with_label_values(&[db.ks_context(ks).name(), "index", read_type.as_ref()])
        .get()
}

#[test]
fn test_index_blob_cache_serves_blobs_outside_window() {
    let dir = tempdir::TempDir::new("test-index-blob-cache").unwrap();
    let key_shape = sixteen_cells();
    let config = with_cache(unmapped_window_config(), 1 << 20);
    write_blobs_outside_window(dir.path(), &key_shape, &config);

    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    let ks = db.single_ks();
    // Cold after open.
    assert_eq!(0, metrics.index_blob_cache_bytes.get());

    // First lookup into cell 3: syscall path, admitted — this lookup is the
    // fill leader and is accounted as a syscall read.
    assert_eq!(Some(vec![3].into()), db.get(ks, &cell_key(3)).unwrap());
    assert_eq!((0, 1, 0), lookup_counts(&metrics, &db, ks));
    assert_eq!(1, metrics.index_blob_cache_fills.get());
    assert_eq!(0, metrics.index_blob_cache_hits.get());
    let blob_bytes = metrics.index_blob_cache_bytes.get();
    assert!(blob_bytes > 0, "the blob must be resident after the fill");

    // Further lookups into the same blob are served from the cache: the
    // key the cell holds (no value LRU here, so it walks the disk levels)
    // and a key it does not hold.
    assert_eq!(Some(vec![3].into()), db.get(ks, &cell_key(3)).unwrap());
    assert_eq!(None, db.get(ks, &[3 << 4, 0, 0, 1]).unwrap());
    assert_eq!((0, 1, 2), lookup_counts(&metrics, &db, ks));
    assert_eq!(1, metrics.index_blob_cache_fills.get());
    assert_eq!(2, metrics.index_blob_cache_hits.get());
    assert_eq!(blob_bytes, metrics.index_blob_cache_bytes.get());

    // Another cell's blob is its own fill.
    assert_eq!(Some(vec![4].into()), db.get(ks, &cell_key(4)).unwrap());
    assert_eq!((0, 2, 2), lookup_counts(&metrics, &db, ks));
    assert_eq!(2, metrics.index_blob_cache_fills.get());
    assert!(metrics.index_blob_cache_bytes.get() > blob_bytes);
    assert_eq!(0, metrics.index_blob_cache_rejected.get());
    drop(db);

    // Restart: the cache starts empty and refills on demand.
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        config.clone(),
        metrics.clone(),
    )
    .unwrap();
    assert_eq!(0, metrics.index_blob_cache_bytes.get());
    assert_eq!(Some(vec![3].into()), db.get(ks, &cell_key(3)).unwrap());
    assert_eq!((0, 1, 0), lookup_counts(&metrics, &db, ks));
    assert_eq!(1, metrics.index_blob_cache_fills.get());
    drop(db);

    // With room to map the history the blobs are served zero-copy from the
    // mmap and never admitted.
    let mut mapped = Config::clone(&config);
    mapped.max_maps = 8;
    let metrics = Metrics::new();
    let db = Db::open(
        dir.path(),
        key_shape.clone(),
        Arc::new(mapped),
        metrics.clone(),
    )
    .unwrap();
    assert_eq!(Some(vec![3].into()), db.get(ks, &cell_key(3)).unwrap());
    assert_eq!((1, 0, 0), lookup_counts(&metrics, &db, ks));
    assert_eq!(0, metrics.index_blob_cache_fills.get());
    assert_eq!(0, metrics.index_blob_cache_bytes.get());
}

#[test]
fn test_index_blob_cache_single_flight_across_lookups() {
    let dir = tempdir::TempDir::new("test-index-blob-cache-single-flight").unwrap();
    let key_shape = sixteen_cells();
    let config = with_cache(unmapped_window_config(), 1 << 20);
    write_blobs_outside_window(dir.path(), &key_shape, &config);

    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.single_ks();

    // Park every lookup right after it dropped the row lock, once all of
    // them are there: each holds its own lazy reader for the same blob and
    // nothing has been read yet, so the fill really is deferred past the
    // lock.
    const LOOKUPS: usize = 4;
    let (latch, guard) = Latch::new();
    let arrivals = Arc::new(AtomicUsize::new(0));
    db.large_table.fp.0.write().fp_lookup_after_lock_drop =
        FailPoint::latch_counting(latch, arrivals.clone());
    let lookups: Vec<_> = (0..LOOKUPS)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || db.get(ks, &cell_key(3)))
        })
        .collect();
    let deadline = Instant::now() + Duration::from_secs(10);
    while arrivals.load(Ordering::SeqCst) < LOOKUPS {
        assert!(
            Instant::now() < deadline,
            "lookups did not all reach the fail point"
        );
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(0, metrics.index_blob_cache_fills.get());
    assert_eq!(0, metrics.index_blob_cache_bytes.get());

    // Release them together: exactly one reads the blob, the others are
    // served by that fill, whether they wait on it or find it resident.
    drop(guard);
    for lookup in lookups {
        assert_eq!(Some(vec![3].into()), lookup.join().unwrap().unwrap());
    }
    assert_eq!(1, metrics.index_blob_cache_fills.get());
    assert_eq!((LOOKUPS - 1) as u64, metrics.index_blob_cache_hits.get());
    assert_eq!(
        (0, 1, (LOOKUPS - 1) as u64),
        lookup_counts(&metrics, &db, ks)
    );
}

#[test]
fn test_index_blob_cache_retires_replaced_blobs() {
    let dir = tempdir::TempDir::new("test-index-blob-cache-retire").unwrap();
    let key_shape = sixteen_cells();
    let config = with_cache(unmapped_window_config(), 1 << 20);
    write_blobs_outside_window(dir.path(), &key_shape, &config);

    let metrics = Metrics::new();
    let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
    let ks = db.single_ks();

    assert_eq!(Some(vec![3].into()), db.get(ks, &cell_key(3)).unwrap());
    assert_eq!(1, metrics.index_blob_cache_fills.get());
    assert!(metrics.index_blob_cache_bytes.get() > 0);
    assert_eq!(0, index_loads(&metrics, &db, ks, ReadType::Cached));

    // Overwrite the key and snapshot. The flush merges the dirty overlay
    // over the cell's L0 — which the flusher loads through the cache rather
    // than from disk — and installs the merged blob at a new position, so
    // the completion retires the cached one.
    db.insert(ks, cell_key(3), vec![33]).unwrap();
    db.force_rebuild_control_region().unwrap();
    assert!(db.large_table.is_all_clean());
    assert_eq!(1, metrics.index_blob_cache_drops.get());
    assert_eq!(0, metrics.index_blob_cache_bytes.get());
    assert_eq!(1, index_loads(&metrics, &db, ks, ReadType::Cached));

    // The replacement sits in the writer's mapped fragment: served zero-copy
    // and not admitted.
    db.large_table.force_unload_clean();
    assert_eq!(Some(vec![33].into()), db.get(ks, &cell_key(3)).unwrap());
    assert_eq!((1, 1, 0), lookup_counts(&metrics, &db, ks));
    assert_eq!(1, metrics.index_blob_cache_fills.get());
    assert_eq!(0, metrics.index_blob_cache_bytes.get());
}

/// Spreads pool indices across the key space: multiplying by an odd
/// constant is a bijection on `u64`, so keys stay distinct while their top
/// bits (which pick the cell) look random.
fn pool_key(i: u64) -> Vec<u8> {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes().to_vec()
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// `(key, value length)` pairs: readable in assertion output.
fn summarize(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<(Vec<u8>, usize)> {
    entries.iter().map(|(k, v)| (k.clone(), v.len())).collect()
}

/// Checks point gets, `exists` and bounded forward and reverse iterators of
/// every database in `dbs` against `model` for a random sample of keys and
/// ranges.
fn verify_against_model(
    dbs: &[Arc<Db>],
    ks: KeySpace,
    model: &Model,
    rng: &mut StdRng,
    key_pool: u64,
    what: &str,
) {
    for _ in 0..60 {
        let k = pool_key(rng.gen_range(0..key_pool));
        let expected = model.get(&k).cloned().map(Bytes::from);
        for (i, db) in dbs.iter().enumerate() {
            assert_eq!(
                expected,
                db.get(ks, &k).unwrap(),
                "{what}: get of {k:?}, db {i}"
            );
            assert_eq!(
                expected.is_some(),
                db.exists(ks, &k).unwrap(),
                "{what}: exists of {k:?}, db {i}"
            );
        }
    }
    for _ in 0..8 {
        let a = pool_key(rng.gen_range(0..key_pool));
        let b = pool_key(rng.gen_range(0..key_pool));
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        let forward: Vec<(Vec<u8>, Vec<u8>)> = model
            .range(lo.clone()..hi.clone())
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for reverse in [false, true] {
            let mut expected = forward.clone();
            if reverse {
                expected.reverse();
            }
            let got: Vec<Vec<(Vec<u8>, Vec<u8>)>> = dbs
                .iter()
                .map(|db| {
                    let mut iterator = db.iterator(ks);
                    iterator.set_lower_bound(lo.clone());
                    iterator.set_upper_bound(hi.clone());
                    if reverse {
                        iterator.reverse();
                    }
                    iterator
                        .map(|item| item.map(|(k, v)| (k.to_vec(), v.to_vec())))
                        .collect::<DbResult<_>>()
                        .unwrap()
                })
                .collect();
            for (i, got_i) in got.iter().enumerate() {
                assert_eq!(
                    summarize(&expected),
                    summarize(got_i),
                    "{what}: iterator reverse={reverse}, db {i} (all dbs: {:?})",
                    got.iter()
                        .map(|entries| summarize(entries))
                        .collect::<Vec<_>>()
                );
                assert_eq!(
                    &expected, got_i,
                    "{what}: iterator reverse={reverse}, db {i}"
                );
            }
        }
    }
}

/// Checks checkpoint point reads and a checkpoint iterator against the model
/// captured when the checkpoint was taken.
fn verify_checkpoints(
    checkpoints: &[Arc<DbCheckpoint>],
    ks: KeySpace,
    model: &Model,
    rng: &mut StdRng,
    key_pool: u64,
    what: &str,
) {
    for _ in 0..30 {
        let k = pool_key(rng.gen_range(0..key_pool));
        let expected = model.get(&k).cloned().map(Bytes::from);
        for (i, checkpoint) in checkpoints.iter().enumerate() {
            assert_eq!(
                expected,
                checkpoint.get(ks, &k).unwrap(),
                "{what}: checkpoint get, db {i}"
            );
        }
    }
    let a = pool_key(rng.gen_range(0..key_pool));
    let b = pool_key(rng.gen_range(0..key_pool));
    let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
    let expected: Vec<(Vec<u8>, Vec<u8>)> = model
        .range(lo.clone()..hi.clone())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    for (i, checkpoint) in checkpoints.iter().enumerate() {
        let mut iterator = checkpoint.iterator(ks);
        iterator.set_lower_bound(lo.clone());
        iterator.set_upper_bound(hi.clone());
        let got: Vec<(Vec<u8>, Vec<u8>)> = iterator
            .map(|item| item.map(|(k, v)| (k.to_vec(), v.to_vec())))
            .collect::<DbResult<_>>()
            .unwrap();
        assert_eq!(expected, got, "{what}: checkpoint iterator, db {i}");
    }
}

/// Runs one deterministic workload against two databases that differ only
/// in whether the index blob cache is enabled, and checks every read (point
/// gets, `exists`, bounded forward and reverse iterators, checkpoint reads)
/// of both against an in-memory model after each round. The configuration
/// keeps the index mmap window at its minimum, promotes and force-relocates
/// aggressively and runs index-based WAL relocation, so blobs keep aging
/// out of the window into the cache and out again through explicit drops,
/// evictions and file reclaim. The second keyspace adds a bloom filter and
/// a value LRU in front of the disk walk.
///
/// Both keyspaces keep the default unloaded iterator. A keyspace with
/// `with_unloaded_iterator(false)` is deliberately not part of this
/// workload: in such a keyspace a checkpoint held across a round of writes
/// and removes while the asynchronous flusher runs loses keys with the
/// cache disabled as well (an engine defect outside this cache), which
/// would fail the comparison for reasons the test is not about.
#[test]
fn test_index_blob_cache_differential() {
    run_differential(false);
}

/// The same workload on auto-sharded cells, so shard blobs flow through
/// the cached readers, the explicit drops diff sharded levels, and
/// relocation re-points shard entries through the sync flush path.
#[test]
fn test_index_blob_cache_differential_sharded() {
    run_differential(true);
}

fn run_differential(sharded: bool) {
    const SEED: u64 = 0x1de_b10b;
    const ROUNDS: usize = 12;
    const OPS_PER_ROUND: usize = 400;
    const KEY_POOL: u64 = 1500;

    let plain = {
        let mut config = Config::small();
        config.frag_size = 32 * 1024;
        config.wal_file_size = 4 * config.frag_size;
        config.max_index_maps = Some(3);
        config.max_dirty_keys = 16;
        config.l0_max_entries = Some(48);
        config.snapshot_unload_threshold = 0;
        config.index_min_occupancy_pct = 50;
        if sharded {
            config.with_index_auto_shard_threshold(2 * 1024);
        }
        config
    };
    // A budget of a few blobs (each is a few KiB here), so the CLOCK sweep
    // runs for certain, while every blob still fits and is admitted.
    let cached = {
        let mut config = plain.clone();
        config.index_blob_cache_bytes = Some(16 * 1024);
        config.index_blob_cache_min_bytes = 1;
        config
    };
    let key_shape = {
        let mut builder = KeyShapeBuilder::new();
        builder.add_key_space("walk", 8, 2, KeyType::uniform(4));
        builder.add_key_space_config(
            "lru",
            8,
            2,
            KeyType::uniform(4),
            KeySpaceConfig::new()
                .with_bloom_filter(0.01, 2000)
                .with_value_cache_size(64),
        );
        builder.build()
    };
    let dirs = [
        tempdir::TempDir::new("test-index-blob-cache-plain").unwrap(),
        tempdir::TempDir::new("test-index-blob-cache-cached").unwrap(),
    ];
    let metrics = [Metrics::new(), Metrics::new()];
    let dbs: Vec<Arc<Db>> = [plain, cached]
        .into_iter()
        .zip(dirs.iter())
        .zip(metrics.iter())
        .map(|((config, dir), metrics)| {
            Db::open(
                dir.path(),
                key_shape.clone(),
                Arc::new(config),
                metrics.clone(),
            )
            .unwrap()
        })
        .collect();
    let kss = [dbs[0].ks("walk"), dbs[0].ks("lru")];
    let mut models: [Model; 2] = Default::default();
    let mut rng = StdRng::seed_from_u64(SEED);

    for round in 0..ROUNDS {
        let what = format!("round {round}");
        // Checkpoints taken before the round's writes must keep reading the
        // pre-round state while the live databases move on.
        let checkpoints: Vec<Arc<DbCheckpoint>> = dbs.iter().map(|db| db.checkpoint()).collect();
        let models_at_checkpoint = models.clone();

        for _ in 0..OPS_PER_ROUND {
            let ks_idx = rng.gen_range(0..2);
            let ks = kss[ks_idx];
            let model = &mut models[ks_idx];
            let k = pool_key(rng.gen_range(0..KEY_POOL));
            match rng.gen_range(0..10) {
                0..=6 => {
                    let v = vec![rng.r#gen::<u8>(); rng.gen_range(16..600)];
                    for db in &dbs {
                        db.insert(ks, k.clone(), v.clone()).unwrap();
                    }
                    model.insert(k, v);
                }
                7 => {
                    for db in &dbs {
                        db.remove(ks, k.clone()).unwrap();
                    }
                    model.remove(&k);
                }
                _ => {
                    let mut writes = Vec::new();
                    for _ in 0..4 {
                        let k = pool_key(rng.gen_range(0..KEY_POOL));
                        let v = vec![rng.r#gen::<u8>(); rng.gen_range(16..600)];
                        writes.push((k, v));
                    }
                    for db in &dbs {
                        let mut batch = db.write_batch();
                        for (k, v) in &writes {
                            batch.write(ks, k.clone(), v.clone());
                        }
                        batch.delete(ks, k.clone());
                        batch.commit().unwrap();
                    }
                    for (k, v) in writes {
                        model.insert(k, v);
                    }
                    model.remove(&k);
                }
            }
        }

        for (ks_idx, ks) in kss.iter().enumerate() {
            verify_checkpoints(
                &checkpoints,
                *ks,
                &models_at_checkpoint[ks_idx],
                &mut rng,
                KEY_POOL,
                &format!("{what} ks {ks_idx}"),
            );
        }
        drop(checkpoints);

        // Flush and unload every dirty cell; force-relocate index files that
        // fell below the occupancy floor; every third round also relocate
        // the older part of the value WAL, which rewrites positions through
        // the sync flush path.
        for db in &dbs {
            db.force_rebuild_control_region().unwrap();
        }
        if round % 3 == 2 {
            for db in &dbs {
                let target = compute_target_position_from_ratio(db, 0.6);
                db.start_blocking_relocation_with_strategy(RelocationStrategy::IndexBased(target));
                db.force_rebuild_control_region().unwrap();
            }
        }
        for (ks_idx, ks) in kss.iter().enumerate() {
            verify_against_model(
                &dbs,
                *ks,
                &models[ks_idx],
                &mut rng,
                KEY_POOL,
                &format!("{what} ks {ks_idx}"),
            );
        }
    }

    // The workload must have exercised what the test is about.
    for (i, metrics) in metrics.iter().enumerate() {
        for ks_name in ["walk", "lru"] {
            assert!(
                metrics.relocation_kept.with_label_values(&[ks_name]).get() > 0,
                "db {i}: relocation must have rewritten positions of {ks_name}"
            );
        }
        assert!(
            metrics
                .snapshot_forced_relocation
                .with_label_values(&["walk"])
                .get()
                > 0,
            "db {i}: force-relocation must have run"
        );
        if sharded {
            assert!(
                metrics
                    .l1_shard_split_total
                    .with_label_values(&["walk"])
                    .get()
                    > 0,
                "db {i}: no L1 was ever split into shards"
            );
        }
    }
    assert_eq!(0, metrics[0].index_blob_cache_fills.get());
    assert!(
        metrics[1].index_blob_cache_fills.get() > 0,
        "no blob was ever cached"
    );
    assert!(
        metrics[1].index_blob_cache_hits.get() > 0,
        "no lookup was served from the cache"
    );
    assert!(
        metrics[1].index_blob_cache_drops.get() > 0,
        "no cached blob was ever retired"
    );
    assert!(
        metrics[1].index_blob_cache_evictions.get() > 0,
        "the budget was never exceeded, so eviction went untested"
    );
    assert_eq!(
        0,
        metrics[1].index_blob_cache_rejected.get(),
        "a blob exceeded the budget and was served uncached"
    );
    assert_eq!(0, metrics[1].index_blob_cache_unreadable.get());
    let (_, syscall, cached) = lookup_counts(&metrics[1], &dbs[1], kss[0]);
    assert!(
        cached > 0 && syscall > 0,
        "both the fill and the hit path must have served lookups"
    );
}
