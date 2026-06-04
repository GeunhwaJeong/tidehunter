// Benchmarks the IndexTable full-scan paths (`keys`, `iter`) that merge the
// flat array with the data overlay. These exercise `iter_combined` and are the
// paths affected by the zero-copy flat-key change (slice_to_bytes vs
// Bytes::from(k.to_vec())) and the removal of the per-flat-element
// `data_contains_key` membership probe.
//
// Run with:
//   cargo bench --features test-utils --bench index_iter_benchmarks

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use tidehunter::WalPosition;
use tidehunter::key_shape::{KeyIndexing, KeyShape, KeySpaceDesc, KeyType};
use tidehunter::minibytes::Bytes;
use tidehunter::test_utils::IndexTable;

/// 32-byte big-endian key so the natural byte order matches the numeric order
/// (serialize/deserialize require sorted, unique keys).
fn make_key(i: usize) -> Bytes {
    let mut k = vec![0u8; 32];
    k[24..].copy_from_slice(&(i as u64).to_be_bytes());
    Bytes::from(k)
}

/// Build a flat-backed IndexTable with `n_flat` entries in `flat` and
/// `n_overlay` entries in the data overlay (colliding with a spread-out subset
/// of the flat keys, so the merge hits the data-shadows-flat tie path).
fn build_table(ks: &KeySpaceDesc, n_flat: usize, n_overlay: usize) -> IndexTable {
    // Stage the entries in `data`, serialize, then deserialize so they land in
    // `flat` (the on-disk blob path), matching a freshly loaded index.
    let mut seed = IndexTable::default();
    for i in 0..n_flat {
        seed.insert(make_key(i), WalPosition::test_value((i as u64) + 1));
    }
    let mut out = BytesMut::new();
    seed.serialize_index_entries(ks, &mut out);
    let mut table = IndexTable::deserialize_index_entries(ks, Bytes::from(out.to_vec()));

    if n_overlay > 0 {
        let step = (n_flat / n_overlay).max(1);
        for j in 0..n_overlay {
            let i = j * step;
            if i >= n_flat {
                break;
            }
            // Higher offset than the flat copy so it is the live position.
            table.insert(
                make_key(i),
                WalPosition::test_value(1_000_000_000 + j as u64),
            );
        }
    }
    table
}

/// Benchmark the `keys`/`iter` scans for one keyspace's index layout. `label`
/// distinguishes the criterion group (`index_iter_fixed32` vs
/// `index_iter_varlen`) so baselines stay comparable per layout.
fn bench_keyspace(c: &mut Criterion, label: &str, ks: &KeySpaceDesc) {
    let mut group = c.benchmark_group(format!("index_iter_{label}"));
    // (flat_entries, overlay_entries)
    for &(n_flat, n_overlay) in &[(10_000usize, 100usize), (100_000usize, 1_000usize)] {
        let table = build_table(ks, n_flat, n_overlay);

        group.bench_with_input(
            BenchmarkId::new("keys", format!("{n_flat}+{n_overlay}")),
            &table,
            |b, table| {
                b.iter(|| {
                    let mut acc = 0usize;
                    for k in table.keys() {
                        acc += k.len();
                    }
                    black_box(acc)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("iter", format!("{n_flat}+{n_overlay}")),
            &table,
            |b, table| {
                b.iter(|| {
                    let mut acc = 0u64;
                    for (k, p) in table.iter() {
                        acc += k.len() as u64;
                        black_box(p);
                    }
                    black_box(acc)
                });
            },
        );
    }
    group.finish();
}

fn bench_index_iter(c: &mut Criterion) {
    // Fixed-length keys are the common case; varlen exercises the offset-table
    // flat format. Both keys are 32 bytes (see `make_key`).
    let layouts = [
        ("fixed32", KeyIndexing::fixed(32)),
        ("varlen", KeyIndexing::variable_length()),
    ];
    for (label, key_indexing) in layouts {
        let (shape, ks_id) = KeyShape::new_single_config_indexing(
            key_indexing,
            16,
            KeyType::uniform(16),
            Default::default(),
        );
        bench_keyspace(c, label, shape.ks(ks_id));
    }
}

criterion_group!(benches, bench_index_iter);
criterion_main!(benches);
