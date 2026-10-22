//! Point-in-time read views over a [`Db`] — see [`DbCheckpoint`].
//!
//! # The unprocessed-write retention invariant
//!
//! A checkpoint captures a WAL frontier `L` (the latched `last_processed`) and
//! reads as of it: in-memory overlay (BTree) positions are filtered to
//! `offset < L`, but on-disk index levels (and the in-memory `flat` buffer) are
//! read **unfiltered** — a checkpoint point read / iterator walks a blob as-is.
//! For that to be correct the on-disk image must never hold a *user* value newer
//! than the frontier. That is guaranteed by this invariant:
//!
//! > Every user-initiated write (anything except a relocation copy) stays in the
//! > BTree overlay until `last_processed` advances past its WAL position. A user
//! > write at position `P` is never promoted to flat nor flushed to disk while it
//! > is unprocessed (`last_processed <= P`); only once `last_processed > P` may it
//! > leave the overlay.
//!
//! `last_processed` is monotonic, and a live checkpoint *pins* it at `L` through
//! the WAL-tracker latch, so while the checkpoint is held nothing it considers
//! post-frontier (`offset >= L`) can reach flat or disk. Reading on-disk levels
//! unfiltered therefore yields the as-of-`L` value.
//!
//! Enforcement points, both keyed on `last_processed`:
//!   - overlay -> flat: `IndexTable::promote_to_flat` (via `merge_into_flat`,
//!     which folds only processed positions);
//!   - overlay -> disk: the flusher calls `IndexTable::retain_processed` on the
//!     blob before writing it. The filter is per-position, so every merge that
//!     feeds it (`IndexTable::merge_dirty`) must fold the overlay's full
//!     position lists — a latest-per-key collapse ahead of the filter would
//!     hand it only a post-frontier position and revert the key to its stale
//!     base entry in the blob. For the same reason the merge keeps the
//!     overlay's `flat` entry for a key the overlay also holds in `data`: when
//!     the filter drops every `data` position of that key, the promoted
//!     `flat` entry is the as-of value the blob must hold.
//!
//! Relocation is the deliberate exception: a relocated copy sits at a WAL-tail
//! offset (`>= L`) yet holds the as-of-frontier *value*, so an unfiltered read
//! still returns the correct value (see [`crate::relocation`]).

use crate::cell::CellId;
use crate::context::DbOpKind;
use crate::db::{Db, DbResult};
use crate::index::index_format::IndexIterCaches;
use crate::iterators::IteratorResult;
use crate::iterators::db_iterator::{DbIterator, IterationSource};
use crate::key_shape::{KeySpace, KeySpaceDesc};
use crate::large_table::GetResult;
use crate::wal::tracker::WalTrackerLatch;
use minibytes::Bytes;
use std::sync::Arc;

/// A stable, point-in-time read view of a [`Db`], created by [`Db::checkpoint`].
///
/// Reads observe the database state as of the WAL frontier captured when the
/// checkpoint was created: values written before the checkpoint are visible,
/// values written afterward are not — even as the underlying database keeps
/// advancing. The view stays stable for the lifetime of this object; dropping
/// it releases the latch and lets the WAL tracker's external frontier catch up.
pub struct DbCheckpoint {
    db: Arc<Db>,
    latch: WalTrackerLatch,
}

impl Drop for DbCheckpoint {
    fn drop(&mut self) {
        // The latch is a cheap value token; clone it to hand an owned copy back
        // to the tracker (the field is behind `&mut self` and cannot be moved).
        self.db.wal_writer.release_latch(self.latch.clone());
    }
}

impl DbCheckpoint {
    pub(crate) fn new(db: Arc<Db>, latch: WalTrackerLatch) -> Self {
        Self { db, latch }
    }

    /// Reads `k` from `ks` as of the checkpoint frontier.
    ///
    /// Only index positions at or below the latched frontier are considered
    /// (see [`crate::large_table::LargeTable::get_checkpoint`]). The bloom
    /// filter and value LRU are reused soundly: the bloom only short-circuits
    /// keys absent from the index, and the LRU is consulted only for keys not
    /// written since the checkpoint.
    pub fn get(&self, ks: KeySpace, k: &[u8]) -> DbResult<Option<Bytes>> {
        let db = self.db.as_ref();
        let context = db.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::Get);
        let reduced_key = context.ks_config.reduce_key(k);
        let last_processed = self.latch.position();
        match db
            .large_table
            .get_checkpoint(context, reduced_key.as_ref(), last_processed, db)?
        {
            // An LRU hit returns the value directly; apply the same index-key
            // check as `Db::get` for keyspaces that reduce keys.
            GetResult::Value(full_key, value) => {
                if context.ks_config.need_check_index_key() && full_key.as_ref() != k {
                    return Ok(None);
                }
                Ok(Some(value))
            }
            GetResult::WalPosition(w) => db.read_record_check_key(context, k, w),
            GetResult::NotFound => Ok(None),
        }
    }

    /// Ordered iterator over the checkpoint's snapshot for `ks`.
    ///
    /// The returned [`DbIterator`] holds an `Arc` to this checkpoint, so the
    /// latch stays pinned and the view stays stable for the iterator's whole
    /// lifetime, independent of writes to the live database.
    pub fn iterator(self: &Arc<Self>, ks: KeySpace) -> DbIterator {
        DbIterator::new(IterationSource::Checkpoint(self.clone()), ks)
    }

    pub(crate) fn ks(&self, ks: KeySpace) -> &KeySpaceDesc {
        self.db.ks_desc(ks)
    }

    pub(crate) fn next_cell(
        &self,
        ks: &KeySpaceDesc,
        cell: &CellId,
        reverse: bool,
    ) -> Option<CellId> {
        self.db.next_cell(ks, cell, reverse)
    }

    /// Checkpoint counterpart of [`Db::next_entry`]: walks the index as of the
    /// latched frontier (overlay positions at or above it are skipped) and
    /// reads matching records from the WAL without touching the value LRU.
    pub(crate) fn next_entry(
        &self,
        ks: KeySpace,
        mut cell: CellId,
        mut prev_key: Option<Bytes>,
        end_cell_exclusive: &Option<CellId>,
        reverse: bool,
        cache: &mut IndexIterCaches,
    ) -> DbResult<Option<IteratorResult<Bytes>>> {
        let db = self.db.as_ref();
        let context = db.ks_context(ks);
        let _timer = context.db_op_timer(DbOpKind::NextEntry);
        let last_processed = self.latch.position();
        loop {
            let Some(result) = db.large_table.next_entry(
                context,
                cell,
                prev_key,
                db,
                end_cell_exclusive,
                reverse,
                cache,
                Some(last_processed),
            )?
            else {
                return Ok(None);
            };
            let (key, value) = match result.value {
                // No LRU is consulted on the checkpoint path, so `Value` is never
                // produced; handle it for completeness.
                GetResult::Value(ref full_key, ref v) => (full_key.clone(), v.clone()),
                GetResult::WalPosition(w) => {
                    match db.read_record_for_indexed_key(context, w, result.key.as_ref())? {
                        // Deliberately no `update_lru`: a checkpoint read must
                        // not seed the live value LRU with as-of values that
                        // may be stale for live reads. (Sharing the
                        // decompressed-batch cache is sound: it is keyed by
                        // WAL position and holds immutable frame bodies, not
                        // per-key values.)
                        Some((k, v)) => (k, v),
                        // Same as `Db::next_entry`: a stale index entry must be
                        // skipped, not treated as the end of the key space.
                        None => {
                            let Some(next_cell) = result.cell.clone() else {
                                return Ok(None);
                            };
                            cell = next_cell;
                            prev_key = Some(result.key.clone());
                            continue;
                        }
                    }
                }
                GetResult::NotFound => unreachable!(),
            };
            return Ok(Some(result.with_key_value(key, value)));
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;
    use crate::db::{Db, DbResult};
    use crate::key_shape::{KeyShape, KeyShapeBuilder, KeySpaceConfig, KeyType};
    use crate::metrics::Metrics;
    use minibytes::Bytes;
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    #[test]
    fn test_checkpoint_snapshot_read() {
        let dir = tempdir::TempDir::new("test-checkpoint").unwrap();
        let config = Arc::new(Config::small());
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let ks = db.single_ks();

        let key = 42u64.to_be_bytes().to_vec();
        let v1: Bytes = vec![1u8].into();
        let v2: Bytes = vec![2u8].into();

        // Insert the original value, then snapshot the database at this point.
        db.insert(ks, key.clone(), v1.clone()).unwrap();
        let checkpoint = db.checkpoint();

        // Update the same key to a new value after the checkpoint was taken.
        db.insert(ks, key.clone(), v2.clone()).unwrap();

        // The checkpoint still observes the value as of when it was created...
        assert_eq!(
            Some(v1.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must return the pre-checkpoint value"
        );
        // ...while a direct read sees the new value.
        assert_eq!(
            Some(v2.clone()),
            db.get(ks, &key).unwrap(),
            "direct read must return the latest value"
        );

        // The snapshot stays stable over time even as background promotion runs:
        // the latch pins the external `last_processed`, so the new value is never
        // promoted past the checkpoint frontier, and the offset filter keeps
        // hiding it.
        thread::sleep(Duration::from_millis(1));
        assert_eq!(
            Some(v1.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must remain stable after waiting"
        );
        assert_eq!(Some(v2.clone()), db.get(ks, &key).unwrap());

        // A second checkpoint taken after the update observes the new value —
        // even while the first checkpoint is still held. Each checkpoint reads
        // as of its own captured frontier; the older latch only pins the
        // externally observed `last_processed`, it does not hold back a newer
        // checkpoint's view.
        let checkpoint2 = db.checkpoint();
        assert_eq!(
            Some(v2.clone()),
            checkpoint2.get(ks, &key).unwrap(),
            "a later checkpoint must observe the new value despite an older one being held"
        );

        // ...and the first checkpoint keeps returning its own snapshot value,
        // unaffected by the existence of the second checkpoint.
        assert_eq!(
            Some(v1.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "the original checkpoint must remain stable alongside a newer one"
        );
    }

    #[test]
    fn test_checkpoint_iterator_snapshot() {
        let dir = tempdir::TempDir::new("test-checkpoint-iter").unwrap();
        let config = Arc::new(Config::small());
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let ks = db.single_ks();

        let k = |n: u64| n.to_be_bytes().to_vec();
        let v = |b: u8| -> Bytes { vec![b].into() };

        // Seed three keys, then snapshot.
        db.insert(ks, k(1), v(10)).unwrap();
        db.insert(ks, k(2), v(20)).unwrap();
        db.insert(ks, k(3), v(30)).unwrap();
        let checkpoint = db.checkpoint();

        // Mutate the live db after the checkpoint: update k1, delete k2, add k4.
        db.insert(ks, k(1), v(11)).unwrap();
        db.remove(ks, k(2)).unwrap();
        db.insert(ks, k(4), v(40)).unwrap();

        let collect = |it: crate::iterators::db_iterator::DbIterator| -> Vec<(Bytes, Bytes)> {
            it.collect::<DbResult<Vec<_>>>().unwrap()
        };

        // The checkpoint iterator yields the snapshot state, ignoring later
        // writes: k1 keeps its old value, the deleted k2 is still present, and
        // the newly inserted k4 is absent.
        let snapshot = collect(checkpoint.iterator(ks));
        assert_eq!(
            snapshot,
            vec![
                (k(1).into(), v(10)),
                (k(2).into(), v(20)),
                (k(3).into(), v(30)),
            ],
            "checkpoint iterator must reflect the snapshot, not later writes"
        );

        // A live iterator reflects the mutations.
        let live = collect(db.iterator(ks));
        assert_eq!(
            live,
            vec![
                (k(1).into(), v(11)),
                (k(3).into(), v(30)),
                (k(4).into(), v(40)),
            ],
            "live iterator must reflect the latest writes"
        );

        // Reverse iteration over the checkpoint yields the snapshot in
        // descending key order.
        let mut rev_iter = checkpoint.iterator(ks);
        rev_iter.reverse();
        assert_eq!(
            collect(rev_iter),
            vec![
                (k(3).into(), v(30)),
                (k(2).into(), v(20)),
                (k(1).into(), v(10)),
            ],
            "reverse checkpoint iterator must reflect the snapshot in reverse"
        );

        // Stable over time even as background promotion runs.
        thread::sleep(Duration::from_millis(1));
        assert_eq!(
            snapshot,
            collect(checkpoint.iterator(ks)),
            "checkpoint iterator must stay stable after waiting"
        );
    }

    /// A pre-checkpoint tombstone (or overwrite) must stay visible to a held
    /// checkpoint when a post-checkpoint write to the same key exists and the
    /// cell is flushed while the latch is held. Covers the `clear_after_flush`
    /// completion path (no write races the flush, so the flushed snapshot is
    /// still shared with the live overlay).
    #[test]
    fn test_checkpoint_survives_flush_with_post_checkpoint_write() {
        let dir = tempdir::TempDir::new("test-checkpoint-flush").unwrap();
        let mut config = Config::small();
        config.snapshot_unload_threshold = 0;
        let config = Arc::new(config);
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let ks = db.single_ks();

        let key_del = 42u64.to_be_bytes().to_vec();
        let key_ovw = 43u64.to_be_bytes().to_vec();
        let v1: Bytes = vec![1u8].into();
        let v1b: Bytes = vec![11u8].into();
        let v2: Bytes = vec![2u8].into();

        // Seed both keys and flush them to a disk L0 (cells unload).
        db.insert(ks, key_del.clone(), v1.clone()).unwrap();
        db.insert(ks, key_ovw.clone(), v1.clone()).unwrap();
        db.force_rebuild_control_region().unwrap();

        // Freshest pre-checkpoint state: key_del deleted, key_ovw overwritten.
        db.remove(ks, key_del.clone()).unwrap();
        db.insert(ks, key_ovw.clone(), v1b.clone()).unwrap();
        let checkpoint = db.checkpoint();

        // Sanity: the checkpoint sees the pre-checkpoint state.
        assert_eq!(None, checkpoint.get(ks, &key_del).unwrap());
        assert_eq!(Some(v1b.clone()), checkpoint.get(ks, &key_ovw).unwrap());

        // Post-checkpoint overwrites, then a flush while the latch is held.
        db.insert(ks, key_del.clone(), v2.clone()).unwrap();
        db.insert(ks, key_ovw.clone(), v2.clone()).unwrap();
        db.rebuild_control_region().unwrap();

        // Live reads see the latest values.
        assert_eq!(Some(v2.clone()), db.get(ks, &key_del).unwrap());
        assert_eq!(Some(v2.clone()), db.get(ks, &key_ovw).unwrap());
        // The checkpoint view must be unchanged by the flush.
        assert_eq!(
            None,
            checkpoint.get(ks, &key_del).unwrap(),
            "checkpoint must still see the pre-checkpoint deletion after a flush"
        );
        assert_eq!(
            Some(v1b),
            checkpoint.get(ks, &key_ovw).unwrap(),
            "checkpoint must still see the freshest pre-checkpoint value after a flush"
        );
    }

    /// Same as above, but a racing write lands while the flush is in flight,
    /// so completion goes through `unmerge_flushed` (per-position removal of
    /// the flushed snapshot from the live overlay). The flushed blob must
    /// contain the as-of-frontier state the unmerge removes — otherwise the
    /// checkpoint read falls through to a stale on-disk value (issue #123).
    #[test]
    fn test_checkpoint_survives_flush_racing_write() {
        use crate::failpoints::FailPoint;
        use crate::latch::Latch;

        let dir = tempdir::TempDir::new("test-checkpoint-flush-race").unwrap();
        let mut config = Config::small();
        config.snapshot_unload_threshold = 0;
        let config = Arc::new(config);
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let metrics = Metrics::new();
        let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
        let ks = db.single_ks();

        let key_del = 42u64.to_be_bytes().to_vec();
        let key_ovw = 43u64.to_be_bytes().to_vec();
        let key_racer = 44u64.to_be_bytes().to_vec();
        let v1: Bytes = vec![1u8].into();
        let v1b: Bytes = vec![11u8].into();
        let v2: Bytes = vec![2u8].into();

        // Seed both keys and flush them to a disk L0 (cells unload).
        db.insert(ks, key_del.clone(), v1.clone()).unwrap();
        db.insert(ks, key_ovw.clone(), v1.clone()).unwrap();
        db.force_rebuild_control_region().unwrap();

        // Freshest pre-checkpoint state: key_del deleted, key_ovw overwritten.
        db.remove(ks, key_del.clone()).unwrap();
        db.insert(ks, key_ovw.clone(), v1b.clone()).unwrap();
        let checkpoint = db.checkpoint();
        assert_eq!(None, checkpoint.get(ks, &key_del).unwrap());
        assert_eq!(Some(v1b.clone()), checkpoint.get(ks, &key_ovw).unwrap());

        // Post-checkpoint overwrites.
        db.insert(ks, key_del.clone(), v2.clone()).unwrap();
        db.insert(ks, key_ovw.clone(), v2.clone()).unwrap();

        // Pause the flusher between the flush work and its completion so the
        // racing write deterministically lands inside the in-flight window.
        // The latch releases when its guard drops — including on a panicking
        // assertion, so a failure cannot wedge the flusher thread.
        let (completion_latch, completion_latch_guard) = Latch::new();
        db.large_table.fp.0.write().fp_flush_before_completion = FailPoint::latch(completion_latch);

        let db2 = db.clone();
        let rebuild = thread::spawn(move || db2.rebuild_control_region().unwrap());

        // Wait until the flusher has picked up the merge flush (the metric
        // increments before the failpoint), then land the racing write. The
        // wait is bounded: on a hung or reclassified flush the assert fails
        // and drops the latch guard, instead of spinning forever with the
        // flusher latched.
        let deadline = Instant::now() + Duration::from_secs(60);
        while metrics.unload.with_label_values(&["merge_flush"]).get() == 0 {
            assert!(
                Instant::now() < deadline,
                "flusher did not reach the merge flush before the deadline"
            );
            thread::sleep(Duration::from_millis(1));
        }
        db.insert(ks, key_racer.clone(), v2.clone()).unwrap();
        drop(completion_latch_guard);
        let _: u64 = rebuild.join().unwrap();

        // Prove the racing write forced the unmerge completion path — the path
        // under test. This depends on key_racer sharing a cell with the other
        // keys (all three u64-BE keys have zero high bytes); if the key layout
        // ever changes, this assert fails instead of the test silently passing
        // through the same_shared path.
        assert!(
            metrics.flush_update.with_label_values(&["unmerge"]).get() >= 1,
            "racing write did not force the unmerge_flushed completion path"
        );

        // Live reads see the latest values.
        assert_eq!(Some(v2.clone()), db.get(ks, &key_del).unwrap());
        assert_eq!(Some(v2.clone()), db.get(ks, &key_ovw).unwrap());
        // The checkpoint view must be unchanged by the flush.
        assert_eq!(
            None,
            checkpoint.get(ks, &key_del).unwrap(),
            "checkpoint must still see the pre-checkpoint deletion after a racing flush"
        );
        assert_eq!(
            Some(v1b),
            checkpoint.get(ks, &key_ovw).unwrap(),
            "checkpoint must still see the freshest pre-checkpoint value after a racing flush"
        );
    }

    #[test]
    fn test_checkpoint_reuses_bloom_and_lru() {
        let dir = tempdir::TempDir::new("test-checkpoint-bloom-lru").unwrap();
        let config = Arc::new(Config::small());
        let mut ksb = KeyShapeBuilder::new();
        let ksc = KeySpaceConfig::new()
            .with_value_cache_size(512)
            .with_bloom_filter(0.01, 2000);
        ksb.add_key_space_config("k", 8, 1, KeyType::uniform(1), ksc);
        let key_shape = ksb.build();
        let metrics = Metrics::new();
        let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
        let ks = db.ks("k");

        let k = |n: u64| n.to_be_bytes().to_vec();
        let v = |b: u8| -> Bytes { vec![b].into() };

        db.insert(ks, k(1), v(10)).unwrap(); // unchanged after the checkpoint
        db.insert(ks, k(2), v(20)).unwrap(); // overwritten after the checkpoint
        let checkpoint = db.checkpoint();
        db.insert(ks, k(2), v(21)).unwrap();

        // k1 is unchanged since the checkpoint, so its value is served from the
        // value LRU without a WAL read — and it is the snapshot value.
        assert_eq!(Some(v(10)), checkpoint.get(ks, &k(1)).unwrap());
        // k2 was overwritten after the checkpoint: the LRU holds the new value
        // (v21) but the freshness guard rejects it, so the checkpoint returns
        // the as-of-frontier value v(20).
        assert_eq!(Some(v(20)), checkpoint.get(ks, &k(2)).unwrap());
        // An absent key is short-circuited by the bloom filter.
        assert_eq!(None, checkpoint.get(ks, &k(999)).unwrap());

        let lru_hits = metrics
            .lookup_result
            .with_label_values(&["k", "found", "lru"])
            .get();
        assert!(
            lru_hits >= 1,
            "checkpoint read of an unchanged key should hit the value LRU"
        );
        let bloom_misses = metrics
            .lookup_result
            .with_label_values(&["k", "not_found", "bloom"])
            .get();
        assert!(
            bloom_misses >= 1,
            "checkpoint read of an absent key should be filtered by the bloom"
        );
    }

    /// A write applied after a newer write to the same key (`WriteOrder::Stale`
    /// in `IndexTable::checked_insert`) is the key's as-of value for
    /// every frontier between its own position and the newer one, so it must
    /// be recorded behind the newer position rather than dropped.
    ///
    /// W1 (`a = v1`, offset p1) and W2 (`b` removed, offset p2) are parked
    /// before taking their row locks, so the tracker frontier cannot pass p1.
    /// A checkpoint is requested behind them: its latch position L covers
    /// p1 and p2 but not the batch write `a = v3` (offset p3) issued after the
    /// request. The batch commits and is promoted first, so W1 is stale when
    /// it applies. The checkpoint at L (p1 < L <= p3) must read `a = v1`
    /// while the live read returns `v3`.
    #[test]
    fn test_checkpoint_sees_stale_write_behind_later_write() {
        use crate::failpoints::FailPoint;
        use crate::latch::Latch;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempdir::TempDir::new("test-checkpoint-stale-write").unwrap();
        let config = Arc::new(Config::small());
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let metrics = Metrics::new();
        let db = Db::open(dir.path(), key_shape, config, metrics.clone()).unwrap();
        let ks = db.single_ks();

        let key_a = 42u64.to_be_bytes().to_vec();
        let key_b = 43u64.to_be_bytes().to_vec();
        let v1: Bytes = vec![1u8].into();
        let v3: Bytes = vec![3u8].into();

        // Park W1 (insert) and W2 (remove) after their WAL allocation, before
        // the row lock: both hold their WAL guards, pinning the frontier at p1.
        let (insert_latch, insert_guard) = Latch::new();
        let (remove_latch, remove_guard) = Latch::new();
        let arrivals = Arc::new(AtomicUsize::new(0));
        db.large_table.fp.0.write().fp_insert_before_lock =
            FailPoint::latch_counting(insert_latch, arrivals.clone());
        db.large_table.fp.0.write().fp_remove_before_lock =
            FailPoint::latch_counting(remove_latch, arrivals.clone());

        let w1 = {
            let db = db.clone();
            let key_a = key_a.clone();
            let v1 = v1.clone();
            thread::spawn(move || db.insert(ks, key_a, v1).unwrap())
        };
        let deadline = Instant::now() + Duration::from_secs(60);
        while arrivals.load(Ordering::SeqCst) < 1 {
            assert!(Instant::now() < deadline, "W1 did not reach the failpoint");
            thread::sleep(Duration::from_millis(1));
        }
        let w2 = {
            let db = db.clone();
            let key_b = key_b.clone();
            thread::spawn(move || db.remove(ks, key_b).unwrap())
        };
        while arrivals.load(Ordering::SeqCst) < 2 {
            assert!(Instant::now() < deadline, "W2 did not reach the failpoint");
            thread::sleep(Duration::from_millis(1));
        }

        // Request the checkpoint now: the tracker answers it only once p1 and
        // p2 are processed, with the frontier captured when it dequeues the
        // request. Wait until the request is queued, so it sits ahead of the
        // batch allocation below (the tracker is FIFO).
        let position_before_batch = db.wal_writer.position();
        let latch_requests_before = db.wal_writer.latch_requests_sent();
        let ckpt = {
            let db = db.clone();
            thread::spawn(move || db.checkpoint())
        };
        while db.wal_writer.latch_requests_sent() == latch_requests_before {
            assert!(Instant::now() < deadline, "checkpoint request was not sent");
            thread::sleep(Duration::from_millis(1));
        }

        // A batch write to `a` lands above the latch position, commits, and is
        // promoted into the overlay before W1 applies.
        let mut batch = db.write_batch();
        batch.write(ks, key_a.clone(), v3.clone());
        batch.commit().unwrap();
        assert_eq!(Some(v3.clone()), db.get(ks, &key_a).unwrap());

        // Release W1: it is stale (p3 > p1) and is ordered behind p3. Then W2.
        drop(insert_guard);
        w1.join().unwrap();
        assert_eq!(
            1,
            metrics
                .skip_stale_update
                .with_label_values(&["root", "insert"])
                .get(),
            "W1 must apply as a stale update behind the promoted batch write"
        );
        drop(remove_guard);
        w2.join().unwrap();
        let checkpoint = ckpt.join().unwrap();

        // The interleaving under test: the latch must not cover the batch write.
        assert!(
            checkpoint.latch.position().as_u64() <= position_before_batch,
            "latch position {} covers the batch write (allocated at or after {}); \
             the checkpoint request was not queued ahead of the batch",
            checkpoint.latch.position().as_u64(),
            position_before_batch
        );

        assert_eq!(Some(v3), db.get(ks, &key_a).unwrap());
        assert_eq!(
            Some(v1),
            checkpoint.get(ks, &key_a).unwrap(),
            "checkpoint must see the write at p1, which precedes its frontier"
        );
    }

    /// The flusher's merge path (`FlushKind::Flush { loaded: false }`) folds the
    /// overlay's `flat` into the loaded L0 with `fold_dirty_flat`. A key held in
    /// the overlay's `flat` (promoted, below the frontier) and `data` (above the
    /// frontier pinned by a checkpoint) must keep its `flat` entry in the merged
    /// table: `retain_processed` removes the `data` entry from the blob, so the
    /// `flat` entry is the as-of value the blob must hold. Completion clears
    /// the overlay's `flat`, so a value dropped by the merge survives nowhere.
    #[test]
    fn test_checkpoint_survives_unloaded_flush_of_promoted_key() {
        let dir = tempdir::TempDir::new("test-checkpoint-promoted-flat-flush").unwrap();
        let config = Arc::new(Config::small());
        let key_shape = KeyShape::new_single(8, 16, KeyType::uniform(16));
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let ks = db.single_ks();
        let context = db.ks_context(ks);

        let key = 42u64.to_be_bytes().to_vec();
        let cell = db.key_shape.ks(ks).cell_id(&key);
        let v1: Bytes = vec![1u8].into();
        let v2: Bytes = vec![2u8].into();
        let v3: Bytes = vec![3u8].into();

        // v1 lands in an on-disk L0; the cell is clean and unloaded afterwards.
        db.insert(ks, key.clone(), v1.clone()).unwrap();
        db.force_rebuild_control_region().unwrap();

        // v2 is the as-of value: written, processed, and promoted into `flat`.
        db.insert(ks, key.clone(), v2.clone()).unwrap();
        db.wal_writer.wal_tracker_barrier();
        db.large_table.test_promote_flat_force(db.as_ref());

        let checkpoint = db.checkpoint();
        assert_eq!(Some(v2.clone()), checkpoint.get(ks, &key).unwrap());

        // v3 is written after the checkpoint: it sits in `data` above the
        // latched frontier while v2 stays in `flat`.
        db.insert(ks, key.clone(), v3.clone()).unwrap();
        assert_eq!(
            db.large_table.entry_state_for_test(context, &cell),
            "DirtyUnloaded"
        );

        // Flush the unloaded cell while the checkpoint pins the frontier.
        db.force_rebuild_control_region().unwrap();

        assert_eq!(Some(v3), db.get(ks, &key).unwrap());
        assert_eq!(
            Some(v2),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must still see the promoted as-of value after the flush"
        );
    }

    /// `maybe_load` folds the overlay into the loaded L0 with the same merge
    /// as the flusher, so a key held in the overlay's `flat` and `data` must
    /// keep its `flat` entry there too: the loaded table serves the
    /// checkpoint's `get_at` directly, and the flush that follows takes the
    /// `flat` entry as the blob's as-of value.
    #[test]
    fn test_checkpoint_survives_load_of_promoted_key() {
        let dir = tempdir::TempDir::new("test-checkpoint-promoted-flat-load").unwrap();
        let config = Arc::new(Config::small());
        // An iterator loads an unloaded cell only with the unloaded iterator off.
        let key_shape = KeyShape::new_single_config(
            8,
            16,
            KeyType::uniform(16),
            KeySpaceConfig::default().with_unloaded_iterator(false),
        );
        let db = Db::open(dir.path(), key_shape, config, Metrics::new()).unwrap();
        let ks = db.single_ks();
        let context = db.ks_context(ks);

        let key = 42u64.to_be_bytes().to_vec();
        let cell = db.key_shape.ks(ks).cell_id(&key);
        let v1: Bytes = vec![1u8].into();
        let v2: Bytes = vec![2u8].into();
        let v3: Bytes = vec![3u8].into();

        // v1 lands in an on-disk L0; the cell is clean and unloaded afterwards.
        db.insert(ks, key.clone(), v1.clone()).unwrap();
        db.force_rebuild_control_region().unwrap();

        // v2 is the as-of value: written, processed, and promoted into `flat`.
        db.insert(ks, key.clone(), v2.clone()).unwrap();
        db.wal_writer.wal_tracker_barrier();
        db.large_table.test_promote_flat_force(db.as_ref());

        let checkpoint = db.checkpoint();
        assert_eq!(Some(v2.clone()), checkpoint.get(ks, &key).unwrap());

        // v3 is written after the checkpoint: it sits in `data` above the
        // latched frontier while v2 stays in `flat`.
        db.insert(ks, key.clone(), v3.clone()).unwrap();
        assert_eq!(
            db.large_table.entry_state_for_test(context, &cell),
            "DirtyUnloaded"
        );

        // A live iterator loads the cell: the overlay folds into L0 in memory.
        let live: Vec<(Bytes, Bytes)> = db.iterator(ks).collect::<DbResult<Vec<_>>>().unwrap();
        assert_eq!(live, vec![(key.clone().into(), v3.clone())]);
        assert_eq!(
            db.large_table.entry_state_for_test(context, &cell),
            "DirtyLoaded"
        );
        assert_eq!(
            Some(v2.clone()),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must still see the promoted as-of value after the load"
        );

        // Flush the loaded cell while the checkpoint pins the frontier.
        db.force_rebuild_control_region().unwrap();

        assert_eq!(Some(v3), db.get(ks, &key).unwrap());
        assert_eq!(
            Some(v2),
            checkpoint.get(ks, &key).unwrap(),
            "checkpoint must still see the promoted as-of value after the flush"
        );
    }
}
