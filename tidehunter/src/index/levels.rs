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
//!
//! When auto-sharding is enabled and a cell's L1 would exceed the WAL
//! fragment size, the L1 slot is "vacated" (set to INVALID) and the
//! `shards` btree owns L1 instead. Each entry in `shards` maps a
//! lower-bound byte key to the `WalPosition` of a per-range L1 sub-blob.
//! See `docs/claude-do-not-read/auto_sharding_l1_design.md`.
use crate::wal::position::WalPosition;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::ops::Bound;

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
/// - Sharded form (`!shards.is_empty()`): `positions[1]` MUST be
///   `WalPosition::INVALID` — the btree owns the L1 data. `positions[0]`
///   (the L0 slot) still behaves normally. `shards` keys are the lower
///   bound of each shard's key range, in strictly increasing byte order,
///   and the smallest key MUST be `Vec::new()` so that every byte key
///   falls in some shard's range under `range(..=k).next_back()`.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexLevels {
    positions: SmallVec<[WalPosition; INLINE_LEVELS]>,
    /// Per-range L1 sub-blobs when auto-sharding has split L1. Empty when
    /// the cell uses the legacy single-blob L1 (the common case). See the
    /// struct-level invariants for the contract this field carries.
    #[serde(default)]
    shards: BTreeMap<Vec<u8>, WalPosition>,
}

impl IndexLevels {
    /// Returns an empty level set (no on-disk blobs).
    pub fn new() -> Self {
        Self {
            positions: SmallVec::new(),
            shards: BTreeMap::new(),
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
        Self {
            positions,
            shards: BTreeMap::new(),
        }
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
        Self {
            positions,
            shards: BTreeMap::new(),
        }
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

    /// Rebuilds an `IndexLevels` from a raw `positions` smallvec deserialized
    /// from the V2 (pre-sharding) control-region format. The resulting cell
    /// is unsharded — `shards` starts empty.
    pub(crate) fn from_legacy_v2_positions(
        positions: SmallVec<[WalPosition; INLINE_LEVELS]>,
    ) -> Self {
        Self {
            positions,
            shards: BTreeMap::new(),
        }
    }

    /// Returns a sharded level set with no L0: L1 has been split into the
    /// per-range blobs in `shards`. Shape: `[INVALID, INVALID]` plus the
    /// btree owning L1.
    ///
    /// Panics (debug) if `shards` is empty, its first key is not the empty
    /// `Vec<u8>` sentinel, or any value is invalid.
    pub fn sharded(shards: BTreeMap<Vec<u8>, WalPosition>) -> Self {
        Self::debug_assert_shards(&shards);
        let mut positions = SmallVec::new();
        positions.push(WalPosition::INVALID);
        positions.push(WalPosition::INVALID);
        Self { positions, shards }
    }

    /// Returns a sharded level set carrying an L0 alongside the shard btree.
    /// Shape: `[l0, INVALID]` plus the btree owning L1.
    pub fn sharded_with_l0(l0: WalPosition, shards: BTreeMap<Vec<u8>, WalPosition>) -> Self {
        debug_assert!(
            l0.is_valid(),
            "IndexLevels::sharded_with_l0 requires a valid L0 position",
        );
        Self::debug_assert_shards(&shards);
        let mut positions = SmallVec::new();
        positions.push(l0);
        positions.push(WalPosition::INVALID);
        Self { positions, shards }
    }

    fn debug_assert_shards(shards: &BTreeMap<Vec<u8>, WalPosition>) {
        debug_assert!(
            !shards.is_empty(),
            "sharded IndexLevels requires at least one shard",
        );
        debug_assert!(
            shards.keys().next().map(|k| k.is_empty()).unwrap_or(false),
            "first shard key must be the empty byte string \
             (sentinel for keyspace minimum)",
        );
        debug_assert!(
            shards.values().all(|p| p.is_valid()),
            "shard positions must all be valid",
        );
    }

    /// True when no level holds a blob. Equivalent to `l0().is_none() && l1().is_none() && ...`
    /// for the populated slots, and considers shard membership too.
    pub fn is_empty(&self) -> bool {
        self.positions.iter().all(|p| !p.is_valid()) && self.shards.is_empty()
    }

    /// Number of schema slots currently tracked (including empty interior slots).
    /// Does not count shards — those live under the L1 slot when sharded.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.positions.len()
    }

    /// Returns the freshest populated level (lowest-index non-INVALID slot
    /// in `positions`). Skips empty interior slots: for `[INVALID, L1]`, this
    /// returns `Some(L1)`.
    ///
    /// For sharded cells, this returns the L0 slot if populated and `None`
    /// otherwise — "freshest" is not meaningful across N disjoint shards,
    /// so we deliberately do not pick one. Callers that need a per-key
    /// answer should use [`shard_for`] instead.
    #[cfg(test)]
    pub fn latest(&self) -> Option<WalPosition> {
        self.positions.iter().copied().find(|p| p.is_valid())
    }

    /// Iterates **populated** levels from freshest (L0) to oldest, skipping
    /// empty interior slots. This is the read-path order: first hit wins.
    /// When sharded, the shard blobs are appended in btree key order after
    /// the (always-INVALID) L1 slot is skipped.
    pub fn iter(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.positions
            .iter()
            .copied()
            .filter(|p| p.is_valid())
            .chain(self.shards.values().copied())
    }

    /// Like [`iter`] but yields `(level_idx, position)` pairs. The level index
    /// is the schema slot (0 = L0, 1 = L1, …), preserved across empty interior
    /// slots — so a post-promote `[INVALID, L1]` yields `(1, L1)`. Shard blobs
    /// are yielded at the L1 slot (level index 1) in btree key order.
    pub fn iter_with_level(&self) -> impl Iterator<Item = (usize, WalPosition)> + '_ {
        self.positions
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, p)| p.is_valid())
            .chain(self.shards.values().copied().map(|p| (1usize, p)))
    }

    /// Iterates **populated** levels below L0 (slot index ≥ 1), skipping
    /// empty interior slots. Used on read paths where L0 content has already
    /// been folded into an in-memory view (e.g., after `maybe_load`) and the
    /// caller needs to consult remaining on-disk levels. When sharded, yields
    /// every shard blob — point-lookup callers that only need one shard should
    /// use [`shard_for`] instead.
    pub fn iter_below_l0(&self) -> impl Iterator<Item = WalPosition> + '_ {
        self.positions
            .iter()
            .copied()
            .skip(1)
            .filter(|p| p.is_valid())
            .chain(self.shards.values().copied())
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
    /// A cell sharded into more than one blob is **not** single-level — the
    /// shard btree contributes multiple populated entries to `iter()`.
    pub fn is_single_level(&self) -> bool {
        self.iter().count() <= 1
    }

    /// True when the L1 slot has been split into per-range shards. The
    /// caller must respect the sharded-form invariants documented on
    /// [`IndexLevels`].
    pub fn is_sharded(&self) -> bool {
        !self.shards.is_empty()
    }

    /// Returns the shard btree. Use [`shard_for`] for the per-key lookup
    /// instead of walking the map directly.
    pub fn shards(&self) -> &BTreeMap<Vec<u8>, WalPosition> {
        &self.shards
    }

    /// Returns the shard `WalPosition` whose range contains `key`. The
    /// btree's first key is the empty byte string (sentinel) so every key
    /// has a covering shard. Returns `None` only when the cell is not
    /// sharded.
    pub fn shard_for(&self, key: &[u8]) -> Option<WalPosition> {
        if self.shards.is_empty() {
            return None;
        }
        // range::<[u8], _>(..=key) finds the greatest btree key ≤ `key`.
        // The sharded-form invariant guarantees a covering entry exists.
        self.shards
            .range::<[u8], _>((Bound::Unbounded, Bound::Included(key)))
            .next_back()
            .map(|(_, p)| *p)
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

    /// Builds a btree with the leading empty-key sentinel and one additional
    /// split point at `mid`.
    fn two_shard_btree(
        p0: WalPosition,
        mid: &[u8],
        p1: WalPosition,
    ) -> BTreeMap<Vec<u8>, WalPosition> {
        let mut m = BTreeMap::new();
        m.insert(Vec::new(), p0);
        m.insert(mid.to_vec(), p1);
        m
    }

    #[test]
    fn sharded_no_l0() {
        let shards = two_shard_btree(pos(10), b"m", pos(20));
        let lvls = IndexLevels::sharded(shards.clone());
        assert!(!lvls.is_empty());
        assert!(lvls.is_sharded());
        assert_eq!(lvls.l0(), None);
        assert_eq!(lvls.l1(), None, "L1 slot is vacated when sharded");
        // iter yields shards in btree order.
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(10), pos(20)]);
        // iter_below_l0 same shape since no L0.
        let below: Vec<_> = lvls.iter_below_l0().collect();
        assert_eq!(below, vec![pos(10), pos(20)]);
        // shard_for: empty key -> first shard; below-mid -> first; at/above-mid -> second.
        assert_eq!(lvls.shard_for(b""), Some(pos(10)));
        assert_eq!(lvls.shard_for(b"a"), Some(pos(10)));
        assert_eq!(lvls.shard_for(b"l"), Some(pos(10)));
        assert_eq!(lvls.shard_for(b"m"), Some(pos(20)));
        assert_eq!(lvls.shard_for(b"n"), Some(pos(20)));
        // is_single_level: two shards ⇒ not single-level.
        assert!(!lvls.is_single_level());
        assert_eq!(lvls.shards(), &shards);
    }

    #[test]
    fn sharded_with_l0_iterates_l0_first() {
        let shards = two_shard_btree(pos(10), b"k", pos(20));
        let lvls = IndexLevels::sharded_with_l0(pos(99), shards);
        assert!(!lvls.is_empty());
        assert!(lvls.is_sharded());
        assert_eq!(lvls.l0(), Some(pos(99)));
        assert_eq!(lvls.l1(), None);
        let collected: Vec<_> = lvls.iter().collect();
        assert_eq!(collected, vec![pos(99), pos(10), pos(20)]);
        // iter_with_level: L0 at 0; shards at 1.
        let levels_pairs: Vec<_> = lvls.iter_with_level().collect();
        assert_eq!(levels_pairs, vec![(0, pos(99)), (1, pos(10)), (1, pos(20))]);
        // iter_below_l0 drops L0.
        let below: Vec<_> = lvls.iter_below_l0().collect();
        assert_eq!(below, vec![pos(10), pos(20)]);
    }

    #[test]
    fn shard_for_returns_none_when_not_sharded() {
        let lvls = IndexLevels::promoted(pos(1));
        assert!(!lvls.is_sharded());
        assert_eq!(lvls.shard_for(b""), None);
        assert_eq!(lvls.shard_for(b"anything"), None);
    }

    #[test]
    fn sharded_serde_roundtrip() {
        // Round-trip through bincode: the SerDe-derived format must preserve
        // both the positions vector and the shard btree intact.
        let shards = two_shard_btree(pos(7), b"split", pos(8));
        let lvls = IndexLevels::sharded_with_l0(pos(3), shards);
        let bytes = bincode::serialize(&lvls).unwrap();
        let back: IndexLevels = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, lvls);
    }

    #[test]
    fn empty_shard_btree_treated_as_unsharded_after_serde() {
        // Default empty shards round-trips fine and behaves like the legacy
        // (unsharded) form.
        let lvls = IndexLevels::single(pos(1));
        let bytes = bincode::serialize(&lvls).unwrap();
        let back: IndexLevels = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back, lvls);
        assert!(!back.is_sharded());
    }

    #[test]
    fn sharded_latest_returns_l0_or_none() {
        let shards = two_shard_btree(pos(10), b"x", pos(20));
        let with_l0 = IndexLevels::sharded_with_l0(pos(99), shards.clone());
        assert_eq!(with_l0.latest(), Some(pos(99)));
        let no_l0 = IndexLevels::sharded(shards);
        // No L0 + sharded: "freshest" is undefined across disjoint shards.
        // latest() returns None so misuse stays loud rather than silently
        // pretending one shard is the answer.
        assert_eq!(no_l0.latest(), None);
    }
}
