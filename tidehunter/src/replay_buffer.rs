//! Replay accumulator that batches per-cell writes during `Db::replay_wal`.
//!
//! The replay loop is single-threaded and processes WAL records in strictly
//! monotonically-increasing offset order. Two properties of replay make a
//! local-buffer approach worthwhile:
//!
//!   1. **No reads.** `entry.data` is write-only during replay (no concurrent
//!      reads, no auto-load of L0). So we can defer all storage updates
//!      until the end of replay.
//!   2. **High overwrite rate.** Many WAL records overwrite the same key
//!      across epochs (especially `objects`). Deduplicating in a HashMap
//!      keyed by `(ks, cell, key)` collapses overwrites in-place — the
//!      latest write wins by virtue of WAL monotonicity and `HashMap::insert`
//!      overwrite semantics.
//!
//! End of replay drains the buffer via [`crate::large_table::LargeTable::apply_replay_buffer`],
//! which takes each cell's row lock exactly once (compare: the per-record
//! path takes the row lock for every WAL record). Per-cell entries are sorted
//! and converted to the flat buffer format in one shot.
//!
//! Compared to the per-record path that drives `IndexTable::checked_insert`
//! per WAL record:
//!   - No `BTreeMap::insert` (O(log N) memcmps per insert).
//!   - No row lock acquired per record.
//!   - No `entry_mut` lookup per record.
//!   - `report_loaded_keys_count` collapsed to one call per cell.

use crate::CellId;
use crate::WalPosition;
use crate::index::index_table::IndexWalPosition;
use crate::key_shape::{KeyShape, KeySpace};
use minibytes::Bytes;
use std::collections::HashMap;

/// Per-cell accumulator: deduplicates writes by key and tracks the first
/// WAL position observed (needed to preserve `mark_dirty`'s `last_processed`
/// invariant — `last_processed` is set on the `Empty → Dirty` transition,
/// which in the per-record path receives the first write's position).
pub(crate) struct CellReplayBuffer {
    pub(crate) entries: HashMap<Bytes, IndexWalPosition>,
    pub(crate) first_position: WalPosition,
}

impl CellReplayBuffer {
    fn new(first_position: WalPosition) -> Self {
        Self {
            entries: HashMap::new(),
            first_position,
        }
    }
}

/// Replay-wide accumulator. Outer slot indexed by keyspace; inner HashMap
/// keyed by `CellId`. Each cell holds its own dedup'd write set.
pub(crate) struct ReplayBuffer {
    by_ks: Vec<HashMap<CellId, CellReplayBuffer>>,
}

impl ReplayBuffer {
    pub(crate) fn new(key_shape: &KeyShape) -> Self {
        let num_keyspaces = key_shape.iter_ks().count();
        let by_ks = (0..num_keyspaces).map(|_| HashMap::new()).collect();
        Self { by_ks }
    }

    pub(crate) fn insert(&mut self, ks: KeySpace, cell: CellId, key: Bytes, position: WalPosition) {
        let cell_buf = self.by_ks[ks.index()]
            .entry(cell)
            .or_insert_with(|| CellReplayBuffer::new(position));
        cell_buf
            .entries
            .insert(key, IndexWalPosition::new_modified(position));
    }

    pub(crate) fn remove(&mut self, ks: KeySpace, cell: CellId, key: Bytes, position: WalPosition) {
        let cell_buf = self.by_ks[ks.index()]
            .entry(cell)
            .or_insert_with(|| CellReplayBuffer::new(position));
        cell_buf
            .entries
            .insert(key, IndexWalPosition::new_removed(position));
    }

    /// Drop any buffered writes for cells in the inclusive range
    /// `[from_cell, to_cell]`. Used by `WalEntry::DropCells` replay: pre-drop
    /// writes sitting in the buffer would otherwise be applied to
    /// `large_table` after `drop_cells_in_range` had already cleared the
    /// cells, resurrecting the dropped data.
    pub(crate) fn drop_cells_in_range(
        &mut self,
        ks: KeySpace,
        from_cell: &CellId,
        to_cell: &CellId,
    ) {
        self.by_ks[ks.index()].retain(|cell, _| !(cell >= from_cell && cell <= to_cell));
    }

    /// Drains this buffer's cells for the given keyspace index. Used by
    /// `LargeTable::apply_replay_buffer` to consume one keyspace at a time.
    pub(crate) fn take_ks(&mut self, ks_idx: usize) -> HashMap<CellId, CellReplayBuffer> {
        std::mem::take(&mut self.by_ks[ks_idx])
    }

    pub(crate) fn num_keyspaces(&self) -> usize {
        self.by_ks.len()
    }
}
