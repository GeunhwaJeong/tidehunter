//! `IndexLevels` — the per-cell list of on-disk index blobs.
//!
//! Today every cell references a single serialized index blob. The two-level
//! LSM design (see `docs/two_level_lsm_design.md`) generalizes this to an
//! ordered list of blobs: L0 (freshest, small, rewritten often), L1 (cold,
//! large, rewritten only on promote), and — schema-wise — further levels if
//! ever needed.
//!
//! This module is deliberately level-generic. The single-level behavior the
//! rest of the codebase currently implements is the `len() ≤ 1` degenerate
//! case. Call sites that genuinely assume "exactly one level" should route
//! through `single()` / `latest()` helpers and carry a `TODO(levels-generic)`
//! comment, so the assumption is explicit and auditable when multi-level
//! flushing lands.
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

/// Ordered list of on-disk index-blob positions for one cell, freshest first.
///
/// Invariants:
/// - `positions[0]` is L0 (most recently flushed, smallest).
/// - `positions[i]` for `i > 0` are older, larger levels, merged into
///   from `i - 1` on promote.
/// - All entries must be valid `WalPosition`s; `WalPosition::INVALID` is
///   never stored — an absent level is represented by shortening the vec.
/// - The empty list represents a cell that has never been flushed.
///
/// The `SmallVec` inline capacity of 2 means the common (and, for the
/// foreseeable future, only) case — one or two levels — never allocates.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexLevels {
    positions: SmallVec<[WalPosition; 2]>,
}

impl IndexLevels {
    /// Returns an empty level set (no on-disk blobs).
    pub fn new() -> Self {
        Self {
            positions: SmallVec::new(),
        }
    }

    /// Returns a level set with a single L0 blob. Used everywhere the
    /// current single-level code would have stored one `WalPosition`.
    pub fn single(position: WalPosition) -> Self {
        debug_assert!(
            position.is_valid(),
            "IndexLevels must not store INVALID positions",
        );
        let mut positions = SmallVec::new();
        positions.push(position);
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

    pub fn is_empty(&self) -> bool {
        self.positions.is_empty()
    }

    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Returns the freshest level (L0) if any.
    ///
    /// `latest()` is the correct accessor for "which blob do I read first"
    /// and "which blob was just written". It is also the right answer to
    /// "which single position to record" in legacy code paths that have not
    /// been generalized yet.
    pub fn latest(&self) -> Option<WalPosition> {
        self.positions.first().copied()
    }

    /// Returns the oldest (largest) level.
    pub fn oldest(&self) -> Option<WalPosition> {
        self.positions.last().copied()
    }

    /// Iterates levels from freshest (L0) to oldest. This is the read-path
    /// order: first hit wins.
    pub fn iter(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.positions.iter().copied()
    }

    /// Consumes the level list and yields owned positions. Useful when the
    /// caller cannot retain the `IndexLevels` (e.g. iterating over an
    /// `entry.levels()` temporary).
    pub fn into_iter_owned(self) -> impl Iterator<Item = WalPosition> {
        self.positions.into_iter()
    }

    /// Returns the position at a specific level index, if present.
    pub fn get(&self, level: usize) -> Option<WalPosition> {
        self.positions.get(level).copied()
    }

    /// Replaces (or appends) the position at `level`.
    ///
    /// Panics if `level > len()` — callers must fill levels contiguously.
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

    /// Drops the blob at level `level`. Subsequent levels shift down by one.
    /// No-op if `level >= len()`.
    pub fn remove(&mut self, level: usize) {
        if level < self.positions.len() {
            self.positions.remove(level);
        }
    }

    /// Truncates the level list to `len` entries.
    pub fn truncate(&mut self, len: usize) {
        self.positions.truncate(len);
    }

    /// Returns `true` if the single-level assumption currently holds.
    ///
    /// Call sites that genuinely need to fall back to single-level
    /// behavior (e.g., legacy flush commands) can assert this.
    pub fn is_single_level(&self) -> bool {
        self.positions.len() <= 1
    }
}

impl From<WalPosition> for IndexLevels {
    fn from(position: WalPosition) -> Self {
        Self::single(position)
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
        assert!(lvls.is_single_level());
    }

    #[test]
    fn single_roundtrip() {
        let lvls = IndexLevels::single(pos(42));
        assert!(!lvls.is_empty());
        assert_eq!(lvls.len(), 1);
        assert_eq!(lvls.latest(), Some(pos(42)));
        assert_eq!(lvls.oldest(), Some(pos(42)));
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
        assert_eq!(lvls.oldest(), Some(pos(2)));
        assert!(!lvls.is_single_level());
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(1), pos(2)]);
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

    #[test]
    fn remove_shifts() {
        let mut lvls = IndexLevels::single(pos(1));
        lvls.set(1, pos(2));
        lvls.remove(0);
        assert_eq!(lvls.len(), 1);
        assert_eq!(lvls.latest(), Some(pos(2)));
    }
}
