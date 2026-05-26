//! Replay accumulator that batches per-cell writes during `Db::replay_wal`.
//!
//! The replay loop is single-threaded and processes WAL records in strictly
//! monotonically-increasing offset order. Two properties of replay make a
//! local-buffer approach worthwhile:
//!
//!   1. **No reads.** `entry.data` is write-only during replay (no concurrent
//!      reads, no auto-load of L0). So we can defer all storage updates
//!      until the end of replay.
//!   2. **Sequential, monotonic WAL.** Each record's position is strictly
//!      greater than the previous one. Per-cell writes accumulate in
//!      insertion order in a `Vec`; a single sort + dedup pass at the end
//!      resolves overwrites (latest position wins per key). And because
//!      entries are pushed in WAL order, `entries[0]` carries the earliest
//!      position observed for the cell — `mark_dirty`'s "first dirty
//!      position" invariant falls out for free.
//!
//! Per-keyspace storage is dispatched on the key shape:
//!   - **Uniform** keyspaces have dense integer cell IDs in `0..num_cells`.
//!     Storage is `Vec<CellReplayBuffer>` pre-sized to `num_cells`, each slot
//!     starting as an empty `Vec`. Direct array indexing, zero hashing on
//!     the hot path; cells never written to stay empty and get filtered out
//!     at drain time.
//!   - **PrefixedUniform** keyspaces have bytes-based cell IDs (variable
//!     prefix). Storage is `HashMap<CellId, CellReplayBuffer>`.
//!
//! End of replay drains the buffer via
//! [`crate::large_table::LargeTable::apply_replay_buffer`], which takes each
//! cell's row lock exactly once (compare: the per-record path takes the row
//! lock for every WAL record). Per-cell entries are sorted, deduped, and
//! converted to the flat buffer format in one shot.

use crate::CellId;
use crate::WalPosition;
use crate::index::index_table::IndexWalPosition;
use crate::key_shape::{KeyShape, KeySpace, KeyType};
use minibytes::Bytes;
use std::collections::HashMap;

/// Per-cell accumulator. Entries are pushed in WAL order; overwrite
/// resolution is deferred to `apply` time via sort + dedup. The first WAL
/// position for the cell (used by `mark_dirty` to anchor `last_processed`)
/// is implicit in `entries[0]`.
pub(crate) type CellReplayBuffer = Vec<(Bytes, IndexWalPosition)>;

/// Per-keyspace storage. Variant chosen at construction time based on the
/// key shape — see module doc.
enum KsReplayBuffer {
    Integer(Vec<CellReplayBuffer>),
    Bytes(HashMap<CellId, CellReplayBuffer>),
}

/// Replay-wide accumulator. Outer slot indexed by keyspace; per-keyspace
/// storage dispatches on key shape (Uniform → Vec, PrefixedUniform → HashMap).
pub(crate) struct ReplayBuffer {
    by_ks: Vec<KsReplayBuffer>,
}

impl ReplayBuffer {
    pub(crate) fn new(key_shape: &KeyShape) -> Self {
        let by_ks = key_shape
            .iter_ks()
            .map(|ksd| match ksd.key_type() {
                KeyType::Uniform(config) => {
                    let num_cells = config.num_cells(ksd);
                    let mut vec = Vec::with_capacity(num_cells);
                    vec.resize_with(num_cells, Vec::new);
                    KsReplayBuffer::Integer(vec)
                }
                KeyType::PrefixedUniform(_) => KsReplayBuffer::Bytes(HashMap::new()),
            })
            .collect();
        Self { by_ks }
    }

    pub(crate) fn insert(&mut self, ks: KeySpace, cell: CellId, key: Bytes, position: WalPosition) {
        // Detach: a zero-copy slice would pin its backing buffer (a
        // multi-MB decompressed `CompressedBatch` body, or a WAL mmap
        // region) for the rest of replay.
        self.cell_mut(ks, cell)
            .push((key.into_owned(), IndexWalPosition::new_modified(position)));
    }

    pub(crate) fn remove(&mut self, ks: KeySpace, cell: CellId, key: Bytes, position: WalPosition) {
        self.cell_mut(ks, cell)
            .push((key.into_owned(), IndexWalPosition::new_removed(position)));
    }

    fn cell_mut(&mut self, ks: KeySpace, cell: CellId) -> &mut CellReplayBuffer {
        match (&mut self.by_ks[ks.index()], cell) {
            (KsReplayBuffer::Integer(vec), CellId::Integer(idx)) => &mut vec[idx],
            (KsReplayBuffer::Bytes(map), cell @ CellId::Bytes(_)) => map.entry(cell).or_default(),
            _ => panic!("CellId variant does not match per-keyspace storage type"),
        }
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
        match (&mut self.by_ks[ks.index()], from_cell, to_cell) {
            (KsReplayBuffer::Integer(vec), CellId::Integer(from), CellId::Integer(to)) => {
                let end = (*to).min(vec.len().saturating_sub(1));
                for slot in vec[*from..=end].iter_mut() {
                    slot.clear();
                }
            }
            (KsReplayBuffer::Bytes(map), CellId::Bytes(_), CellId::Bytes(_)) => {
                map.retain(|cell, _| !(cell >= from_cell && cell <= to_cell));
            }
            _ => panic!("CellId variant does not match per-keyspace storage type"),
        }
    }

    /// Drain this buffer's cells for the given keyspace index. Empty cells
    /// are filtered out so the apply path doesn't take a row lock just to
    /// discover there's no work. Returns a flat `Vec<(CellId, CellReplayBuffer)>`
    /// regardless of underlying storage variant — apply code can group by
    /// mutex shard uniformly.
    pub(crate) fn take_ks(&mut self, ks_idx: usize) -> Vec<(CellId, CellReplayBuffer)> {
        // Replace the variant with an empty Bytes-style placeholder; subsequent
        // accesses for this ks are not expected (apply drains each ks once).
        let placeholder = KsReplayBuffer::Bytes(HashMap::new());
        match std::mem::replace(&mut self.by_ks[ks_idx], placeholder) {
            KsReplayBuffer::Integer(vec) => vec
                .into_iter()
                .enumerate()
                .filter(|(_, buf)| !buf.is_empty())
                .map(|(idx, buf)| (CellId::Integer(idx), buf))
                .collect(),
            KsReplayBuffer::Bytes(map) => map.into_iter().collect(),
        }
    }

    pub(crate) fn num_keyspaces(&self) -> usize {
        self.by_ks.len()
    }
}
