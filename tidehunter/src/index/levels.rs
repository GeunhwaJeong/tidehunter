//! `IndexLevels` — the per-cell list of on-disk index blobs.
//!
//! The two-level LSM design represents each cell's on-disk state as an
//! ordered list of level slots: L0 (freshest, small, rewritten often) at
//! index 0, L1 (cold, large, rewritten only on promote) at index 1, and —
//! schema-wise — further levels if ever needed.
//!
//! A slot can be "empty" even when a later slot is populated: immediately
//! after a promote, L0 is empty and L1 holds the merged blob. To represent
//! that unambiguously (without depending on size-based classification at
//! read-time) we store `WalPosition::INVALID` as a sentinel for empty
//! interior slots. Trailing INVALIDs do not occur — all constructors
//! produce canonical form and `set()` only accepts valid positions.
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Inline SmallVec capacity used by all per-level containers (the level list
/// itself, plus matching buffers of readers/caches built alongside it in
/// `large_table` and `index_format`). Sized to the two-level default policy
/// so the common case — L0 only, or L0+L1 — never allocates. Not a hard cap:
/// SmallVec spills to the heap if a cell ever carries more levels.
pub const INLINE_LEVELS: usize = 2;

/// Ordered list of on-disk index-blob positions for one cell, freshest first.
///
/// Invariants:
/// - `positions[0]` is the L0 slot, `positions[1]` is L1, etc.
/// - An entry of `WalPosition::INVALID` means "this level slot exists in the
///   schema but currently holds no blob" (e.g., L0 right after a promote).
/// - No trailing INVALIDs: `[L0]` is the only form of "L0 only"; `[L0, INVALID]`
///   never occurs. Interior INVALIDs — typically `[INVALID, L1]` post-promote —
///   are valid and preserved.
/// - The empty list represents a cell that has never been flushed.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexLevels {
    positions: SmallVec<[WalPosition; INLINE_LEVELS]>,
}

impl IndexLevels {
    /// Returns an empty level set (no on-disk blobs).
    pub fn new() -> Self {
        Self {
            positions: SmallVec::new(),
        }
    }

    /// Returns a level set with a single L0 blob.
    pub fn single(position: WalPosition) -> Self {
        debug_assert!(
            position.is_valid(),
            "IndexLevels::single requires a valid position",
        );
        let mut positions = SmallVec::new();
        positions.push(position);
        Self { positions }
    }

    /// Returns a post-promote level set: L0 is empty (sentinel), L1 holds
    /// the promoted blob. Shape: `[INVALID, l1]`.
    pub fn promoted(l1: WalPosition) -> Self {
        debug_assert!(
            l1.is_valid(),
            "IndexLevels::promoted requires a valid L1 position",
        );
        let mut positions = SmallVec::new();
        positions.push(WalPosition::INVALID);
        positions.push(l1);
        Self { positions }
    }

    /// Constructs from a possibly-invalid `WalPosition`. Used in the
    /// migration path from `SnapshotEntryData` where `WalPosition::INVALID`
    /// historically represented "no blob"; callers should prefer `new()` or
    /// `single()` for fresh code.
    pub fn from_legacy_position(position: WalPosition) -> Self {
        if position.is_valid() {
            Self::single(position)
        } else {
            Self::new()
        }
    }

    /// True when no level holds a blob. Equivalent to `l0().is_none() && l1().is_none() && ...`
    /// for the populated slots.
    pub fn is_empty(&self) -> bool {
        self.positions.iter().all(|p| !p.is_valid())
    }

    /// Number of schema slots currently tracked (including empty interior slots).
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Returns the freshest populated level (lowest-index non-INVALID).
    ///
    /// Skips empty interior slots: for `[INVALID, L1]`, this returns `Some(L1)`.
    pub fn latest(&self) -> Option<WalPosition> {
        self.positions.iter().copied().find(|p| p.is_valid())
    }

    /// Iterates **populated** levels from freshest (L0) to oldest, skipping
    /// empty interior slots. This is the read-path order: first hit wins.
    pub fn iter(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.positions.iter().copied().filter(|p| p.is_valid())
    }

    /// Like [`iter`] but yields `(level_idx, position)` pairs. The level index
    /// is the schema slot (0 = L0, 1 = L1, …), preserved across empty interior
    /// slots — so a post-promote `[INVALID, L1]` yields `(1, L1)`.
    pub fn iter_with_level(&self) -> impl Iterator<Item = (usize, WalPosition)> + '_ {
        self.positions
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, p)| p.is_valid())
    }

    /// Iterates **populated** levels below L0 (slot index ≥ 1), skipping
    /// empty interior slots. Used on read paths where L0 content has already
    /// been folded into an in-memory view (e.g., after `maybe_load`) and the
    /// caller needs to consult remaining on-disk levels.
    pub fn iter_below_l0(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.positions
            .iter()
            .copied()
            .skip(1)
            .filter(|p| p.is_valid())
    }

    /// Returns the position at a specific level slot, if populated.
    /// INVALID slot or out-of-bounds both return `None`.
    pub fn get(&self, level: usize) -> Option<WalPosition> {
        self.positions.get(level).copied().filter(|p| p.is_valid())
    }

    /// Convenience for the L0 slot.
    pub fn l0(&self) -> Option<WalPosition> {
        self.get(0)
    }

    /// Convenience for the L1 slot.
    pub fn l1(&self) -> Option<WalPosition> {
        self.get(1)
    }

    /// Replaces (or appends) the position at `level`. `position` must be valid.
    ///
    /// Panics if `level` skips past the end — callers must fill levels contiguously.
    pub fn set(&mut self, level: usize, position: WalPosition) {
        debug_assert!(position.is_valid());
        assert!(
            level <= self.positions.len(),
            "level {level} skips past end of levels (len = {})",
            self.positions.len(),
        );
        if level == self.positions.len() {
            self.positions.push(position);
        } else {
            self.positions[level] = position;
        }
    }

    /// True when at most one slot is populated (post-promote `[INVALID, L1]`
    /// counts as single-level since only L1 holds data). Used by call sites
    /// that need to assert the L0-only or L1-only invariant explicitly.
    pub fn is_single_level(&self) -> bool {
        self.iter().count() <= 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::position::WalPosition;

    fn pos(v: u64) -> WalPosition {
        WalPosition::test_value(v)
    }

    #[test]
    fn empty_roundtrip() {
        let lvls = IndexLevels::new();
        assert!(lvls.is_empty());
        assert_eq!(lvls.len(), 0);
        assert_eq!(lvls.latest(), None);
        assert_eq!(lvls.l0(), None);
        assert_eq!(lvls.l1(), None);
        assert!(lvls.is_single_level());
    }

    #[test]
    fn single_roundtrip() {
        let lvls = IndexLevels::single(pos(42));
        assert!(!lvls.is_empty());
        assert_eq!(lvls.len(), 1);
        assert_eq!(lvls.latest(), Some(pos(42)));
        assert_eq!(lvls.l0(), Some(pos(42)));
        assert_eq!(lvls.l1(), None);
        assert!(lvls.is_single_level());
    }

    #[test]
    fn legacy_invalid_is_empty() {
        let lvls = IndexLevels::from_legacy_position(WalPosition::INVALID);
        assert!(lvls.is_empty());
    }

    #[test]
    fn two_levels() {
        let mut lvls = IndexLevels::single(pos(1));
        lvls.set(1, pos(2));
        assert_eq!(lvls.len(), 2);
        assert_eq!(lvls.latest(), Some(pos(1)));
        assert_eq!(lvls.l0(), Some(pos(1)));
        assert_eq!(lvls.l1(), Some(pos(2)));
        assert!(!lvls.is_single_level());
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(1), pos(2)]);
    }

    #[test]
    fn promoted_post_promote_state() {
        let lvls = IndexLevels::promoted(pos(99));
        assert!(!lvls.is_empty());
        assert_eq!(lvls.len(), 2);
        assert_eq!(lvls.l0(), None);
        assert_eq!(lvls.l1(), Some(pos(99)));
        assert_eq!(lvls.latest(), Some(pos(99)));
        assert!(lvls.is_single_level());
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(99)]);
    }

    #[test]
    fn set_replaces() {
        let mut lvls = IndexLevels::single(pos(1));
        lvls.set(0, pos(10));
        assert_eq!(lvls.latest(), Some(pos(10)));
        assert_eq!(lvls.len(), 1);
    }

    #[test]
    #[should_panic]
    fn set_past_end_panics() {
        let mut lvls = IndexLevels::new();
        lvls.set(1, pos(1));
    }
}
