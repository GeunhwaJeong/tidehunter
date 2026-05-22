//! WAL replay driver.
//!
//! Drives [`WalIterator`] through every WAL frame and accumulates per-cell
//! writes into a [`ReplayBuffer`]. At end-of-stream the buffer is drained
//! into the [`LargeTable`] in bulk via `apply_replay_buffer`:
//!
//!   - one row lock per mutex shard instead of one per WAL record,
//!   - one per-cell sort + flat-buffer build instead of per-record BTreeMap
//!     inserts (see `replay_buffer` for the per-cell accumulator).
//!
//! Lives in its own module so `db.rs` doesn't carry the loop body.

use crate::compressed_batch::{InnerIter, decompress_wal_entry};
use crate::context::KsContextVec;
use crate::db::{DbResult, WalEntry};
use crate::key_shape::KeyShape;
use crate::large_table::LargeTable;
use crate::metrics::Metrics;
use crate::replay_buffer::ReplayBuffer;
use crate::wal::{WalError, WalIterator, WalWriter};
use std::collections::VecDeque;

pub(crate) fn replay_wal(
    contexts: &KsContextVec,
    large_table: &LargeTable,
    key_shape: &KeyShape,
    mut wal_iterator: WalIterator,
    metrics: &Metrics,
) -> DbResult<WalWriter> {
    let mut buffer = ReplayBuffer::new(key_shape);
    // Buffer for `BatchStart` collections — holds decoded entries inside a
    // single user-level batch until the batch is fully collected and we can
    // apply its records.
    let mut batch: VecDeque<(crate::WalPosition, WalEntry)> = VecDeque::new();
    let mut batch_remaining: u32 = 0;
    let mut batch_start_position: Option<u64> = None;

    let writer = loop {
        let (position, entry) = if batch_remaining == 0 && !batch.is_empty() {
            // Finished collecting a BatchStart group; emit its records.
            batch_start_position = None;
            batch.pop_front().expect("invariant checked")
        } else {
            let entry = wal_iterator.next();
            if matches!(entry, Err(WalError::Crc(_) | WalError::EndOfWal)) {
                break wal_iterator.into_writer(batch_start_position);
            }
            let (position, raw_entry) = entry?;
            let entry = WalEntry::from_bytes(raw_entry);
            (position, entry)
        };
        if batch_remaining > 0 {
            assert!(
                matches!(entry, WalEntry::Record(..) | WalEntry::Remove(..)),
                "encountered entry {entry:?} at position {position:?} during replay, while expected record or tombstone"
            );
            batch_remaining -= 1;
            batch.push_back((position, entry));
            continue;
        }
        match entry {
            WalEntry::Record(ks, k, _v, relocated) => {
                metrics.replayed_wal_records.inc();
                if relocated {
                    // Nothing needs to be done for the relocated record
                    continue;
                }
                let context = contexts.ks_context(ks);
                let reduced_key = context.ks_config.reduced_key_bytes(k);
                let cell = context.ks_config.cell_id(&reduced_key);
                buffer.insert(ks, cell, reduced_key, position);
            }
            WalEntry::Index(_ks, _bytes) => {
                unreachable!("Should not have index entries in wal");
            }
            WalEntry::Remove(ks, k) => {
                metrics.replayed_wal_records.inc();
                let context = contexts.ks_context(ks);
                let reduced_key = context.ks_config.reduced_key_bytes(k);
                let cell = context.ks_config.cell_id(&reduced_key);
                buffer.remove(ks, cell, reduced_key, position);
            }
            WalEntry::BatchStart(size) => {
                batch_start_position = Some(position.offset());
                batch = VecDeque::with_capacity(size as usize);
                batch_remaining = size;
            }
            WalEntry::DropCells(ks, from_cell, to_cell) => {
                metrics.replayed_wal_records.inc();
                // Drop on both sides: the buffer (so pre-drop writes don't
                // resurrect at apply time) and `large_table` (in case the
                // cells exist as Unloaded entries from a CR snapshot loaded
                // at replay start).
                buffer.drop_cells_in_range(ks, &from_cell, &to_cell);
                let context = contexts.ks_context(ks);
                large_table.drop_cells_in_range(context, &from_cell, &to_cell);
            }
            entry @ WalEntry::CompressedBatch(..) => {
                // A compressed batch is one durable WAL frame — either it
                // CRC-verifies (full batch present) or it does not. There is
                // no half-state, so we apply every inner entry
                // unconditionally; all of them share `position`.
                let decompressed = decompress_wal_entry(entry).expect("matched above");
                for inner in InnerIter::new(decompressed) {
                    metrics.replayed_wal_records.inc();
                    match inner {
                        WalEntry::Record(ks, key, _value, _relocated) => {
                            let context = contexts.ks_context(ks);
                            let reduced_key = context.ks_config.reduced_key_bytes(key);
                            let cell = context.ks_config.cell_id(&reduced_key);
                            buffer.insert(ks, cell, reduced_key, position);
                        }
                        WalEntry::Remove(ks, key) => {
                            let context = contexts.ks_context(ks);
                            let reduced_key = context.ks_config.reduced_key_bytes(key);
                            let cell = context.ks_config.cell_id(&reduced_key);
                            buffer.remove(ks, cell, reduced_key, position);
                        }
                        other => panic!(
                            "Unexpected inner entry in CompressedBatch at {position:?}: {other:?}"
                        ),
                    }
                }
            }
        }
    };
    large_table.apply_replay_buffer(buffer);
    Ok(writer)
}
