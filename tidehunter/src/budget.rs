//! Per-cell index space budget.
//!
//! `Budget` tracks how many more bytes each cell can absorb before its merged
//! index would exceed `frag_size - margin` (the per-cell limit the flusher
//! enforces). Inserts/removes call [`Budget::try_consume`] *before* the WAL
//! write; if the decrement would drive the cell's remaining budget below
//! zero, the consume is undone and the caller returns
//! `DbError::IndexWouldOverflow` — so the WAL never receives records that
//! would later panic the flusher.
//!
//! Lookups are lock-free: the cell map is held in an [`ArcSwap`] of an
//! immutable `HashMap<CellId, Arc<AtomicU64>>`, and per-cell counters are
//! atomic. Structural changes (adding/removing cells, e.g. when a
//! PrefixedUniform keyspace creates a new cell on first write or
//! `drop_cells_in_range` removes one) are serialised by a small mutex and
//! publish a fresh map via `ArcSwap::store`. Existing readers keep working
//! against their captured `Arc<AtomicU64>`s after the swap.
//!
//! Lifecycle hooks are called by the caller (Uniform cells are populated
//! once at construction; PrefixedUniform cells are added in
//! `LargeTable::entry_mut` when the cell is first created and removed in
//! `drop_cells_in_range`). `try_consume` does not lazy-create — if a cell
//! isn't tracked yet, the consume is a no-op for that op only.
//!
//! The check is over-conservative on purpose:
//! - Failed batch commits do not refund the bytes consumed by ops that
//!   succeeded before the failing op. This is rare and the over-count is
//!   bounded by one batch's worth of keys per affected cell.
//! - Tombstone bytes consumed by `remove` aren't refunded when the deepest
//!   level is later flushed and the tombstones strip out — only `reset`
//!   reconciles the budget against the on-disk projection.
//!
//! Disabled by `Config::skip_space_budget`. When skipped, every method is a
//! no-op and the legacy "panic in flusher on overflow" behavior is restored.

use crate::cell::CellId;
use arc_swap::ArcSwap;
use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Per-cell space-budget tracker. See module docs for the full picture.
pub(crate) struct Budget {
    /// Cell -> remaining-bytes counter. Swapped wholesale on add/remove.
    cells: ArcSwap<HashMap<CellId, Arc<AtomicU64>>>,
    /// Serialises map structural changes (add/remove). The atomic *values*
    /// inside live cells are mutated concurrently without this lock.
    update_mutex: Mutex<()>,
    /// Per-cell starting budget = `frag_size - margin`. Used to refill on
    /// `reset` and to seed new cells on `add_cell`.
    limit: u64,
    /// When true, all methods are no-ops; callers see `Ok(())` from
    /// `try_consume`. Mirrors `Config::skip_space_budget`.
    skip: bool,
}

/// Returned by [`Budget::try_consume`] when a cell would overflow. Carries
/// just enough context to build a `DbError::IndexWouldOverflow`.
pub(crate) struct BudgetOverflow {
    pub bytes: u64,
    pub limit_bytes: u64,
}

impl Budget {
    /// `initial_remaining[cell]` is the cell's starting remaining budget,
    /// usually `limit - combined_index_size`. Cells absent from the map go
    /// untracked until [`Self::add_cell`] is called for them — see the
    /// PrefixedUniform path in `LargeTable::entry_mut`.
    pub(crate) fn new(initial_remaining: HashMap<CellId, u64>, limit: u64, skip: bool) -> Self {
        let cells: HashMap<_, _> = initial_remaining
            .into_iter()
            .map(|(k, v)| (k, Arc::new(AtomicU64::new(v))))
            .collect();
        Self {
            cells: ArcSwap::from_pointee(cells),
            update_mutex: Mutex::new(()),
            limit,
            skip,
        }
    }

    /// Atomically decrement the cell's remaining budget. Uses a CAS-loop
    /// (`fetch_update`) so the counter only ever holds a non-negative
    /// value: on overflow no decrement is published, no undo is required,
    /// and concurrent callers never observe a transient negative state.
    ///
    /// `bytes` is the conservative byte cost that the caller's op will add
    /// to the cell's merged index — typically the (reduced) key length, with
    /// a margin baked into `limit` to absorb per-entry serialisation
    /// overhead the projection doesn't count directly.
    ///
    /// If `cell_id` isn't tracked yet, this returns `Ok(())` without
    /// decrementing. The only path that reaches `try_consume` for an
    /// untracked cell is the very first insert into a freshly created
    /// PrefixedUniform cell: cells are registered by `LargeTable::entry_mut`
    /// when the cell is created in the entries map, which runs after this
    /// pre-WAL check. Subsequent ops on the same cell find it tracked.
    pub(crate) fn try_consume(&self, cell_id: &CellId, bytes: u64) -> Result<(), BudgetOverflow> {
        if self.skip {
            return Ok(());
        }
        let map = self.cells.load();
        let Some(atomic) = map.get(cell_id) else {
            return Ok(());
        };
        match atomic.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
            prev.checked_sub(bytes)
        }) {
            Ok(_) => Ok(()),
            Err(_) => Err(BudgetOverflow {
                bytes,
                limit_bytes: self.limit,
            }),
        }
    }

    /// Set the cell's remaining budget to `limit - used_bytes`. Called after
    /// a flush completes to reconcile the budget against the actual on-disk
    /// merged-index size (which may have shrunk via tombstone strip-out or
    /// duplicate-key collapse during promote).
    pub(crate) fn reset(&self, cell_id: &CellId, used_bytes: u64) {
        if self.skip {
            return;
        }
        let map = self.cells.load();
        if let Some(atomic) = map.get(cell_id) {
            atomic.store(self.limit.saturating_sub(used_bytes), Ordering::Relaxed);
        }
        // If the cell isn't tracked yet, leave it — it will be created with
        // full budget on the next `add_cell`, which is what `used_bytes`
        // would imply for a fresh cell anyway.
    }

    /// Add a cell with full remaining budget. Idempotent. Used by
    /// `LargeTable::entry_mut` when a PrefixedUniform cell is created in
    /// the entries map. The fast path (cell already tracked) is a single
    /// `ArcSwap` load + `contains_key` so per-insert overhead stays low.
    pub(crate) fn add_cell(&self, cell_id: CellId) {
        if self.skip {
            return;
        }
        // Fast path: lock-free check.
        if self.cells.load().contains_key(&cell_id) {
            return;
        }
        let _guard = self.update_mutex.lock();
        // Recheck under the mutex in case a concurrent caller raced us in.
        let current = self.cells.load();
        if current.contains_key(&cell_id) {
            return;
        }
        let mut new_map = HashMap::with_capacity(current.len() + 1);
        for (k, v) in current.iter() {
            new_map.insert(k.clone(), v.clone());
        }
        new_map.insert(cell_id, Arc::new(AtomicU64::new(self.limit)));
        self.cells.store(Arc::new(new_map));
    }

    /// Remove the cell from the budget map. Used by `drop_cells_in_range`.
    pub(crate) fn remove_cell(&self, cell_id: &CellId) {
        if self.skip {
            return;
        }
        let _guard = self.update_mutex.lock();
        let current = self.cells.load();
        if !current.contains_key(cell_id) {
            return;
        }
        let mut new_map = HashMap::with_capacity(current.len().saturating_sub(1));
        for (k, v) in current.iter() {
            if k != cell_id {
                new_map.insert(k.clone(), v.clone());
            }
        }
        self.cells.store(Arc::new(new_map));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cid(v: usize) -> CellId {
        CellId::Integer(v)
    }

    #[test]
    fn consume_within_limit() {
        let mut init = HashMap::new();
        init.insert(cid(0), 100);
        let b = Budget::new(init, 100, false);
        assert!(b.try_consume(&cid(0), 40).is_ok());
        assert!(b.try_consume(&cid(0), 40).is_ok());
        let err = b.try_consume(&cid(0), 40).unwrap_err();
        assert_eq!(err.bytes, 40);
    }

    #[test]
    fn overflow_does_not_decrement() {
        let mut init = HashMap::new();
        init.insert(cid(0), 10);
        let b = Budget::new(init, 10, false);
        assert!(b.try_consume(&cid(0), 5).is_ok());
        assert!(b.try_consume(&cid(0), 10).is_err());
        // The failing consume left the counter intact, so another 5 still fits.
        assert!(b.try_consume(&cid(0), 5).is_ok());
    }

    #[test]
    fn add_cell_brings_cell_under_tracking() {
        // The Tree-cell creation hook adds the cell with full budget;
        // subsequent consumes are gated normally.
        let b = Budget::new(HashMap::new(), 100, false);
        b.add_cell(cid(7));
        assert!(b.try_consume(&cid(7), 80).is_ok());
        assert!(b.try_consume(&cid(7), 30).is_err());
    }

    #[test]
    fn reset_refills_after_flush() {
        let mut init = HashMap::new();
        init.insert(cid(0), 0);
        let b = Budget::new(init, 100, false);
        assert!(b.try_consume(&cid(0), 1).is_err());
        b.reset(&cid(0), 20);
        assert!(b.try_consume(&cid(0), 70).is_ok());
        assert!(b.try_consume(&cid(0), 20).is_err());
    }

    #[test]
    fn skip_disables_check() {
        let b = Budget::new(HashMap::new(), 0, true);
        // Even with zero limit, consume succeeds when skipped.
        for _ in 0..1000 {
            assert!(b.try_consume(&cid(0), 1).is_ok());
        }
    }

    #[test]
    fn remove_then_readd_resets_to_full_budget() {
        let mut init = HashMap::new();
        init.insert(cid(0), 50);
        let b = Budget::new(init, 100, false);
        assert!(b.try_consume(&cid(0), 40).is_ok());
        b.remove_cell(&cid(0));
        b.add_cell(cid(0));
        // Re-added with full budget.
        assert!(b.try_consume(&cid(0), 80).is_ok());
        assert!(b.try_consume(&cid(0), 30).is_err());
    }
}
