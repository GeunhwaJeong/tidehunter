use crate::index::index_format::Direction;
use crate::wal::position::WalPosition;
use minibytes::Bytes;
use std::cmp::Ordering;

/// Represents the possible outcomes when looking for the next entry
#[derive(Debug, PartialEq)]
pub enum NextEntryResult {
    /// No entry was found
    NotFound,
    /// A valid entry was found with its key and position
    Found(Bytes, WalPosition),
    /// A deleted entry was found that should be skipped
    SkipDeleted(Bytes),
}

/// k-way merge of ordered (key, position) candidates from multiple sources,
/// used by the iterator to walk an LSM level stack (in-memory overlay plus
/// L0, L1, ...).
///
/// Priority ordering is expressed by iteration order: the first source yielded
/// wins ties on equal keys. Typical call sites pass the in-memory overlay
/// first, then L0 (freshest) through older levels — so fresher data shadows
/// staler data, as required by the LSM read semantics.
///
/// - `NotFound`: every source was `None`.
/// - `Found(k, v)`: winning candidate has a valid position.
/// - `SkipDeleted(k)`: winning candidate is a tombstone (`WalPosition::INVALID`).
///   Callers advance `prev_key` past `k` and retry — the winning source's
///   tombstone shadows any lower-priority hits at the same key.
pub fn merge_levels_next_entry<I>(candidates: I, direction: Direction) -> NextEntryResult
where
    I: IntoIterator<Item = Option<(Bytes, WalPosition)>>,
{
    let mut winner: Option<(Bytes, WalPosition)> = None;
    for candidate in candidates {
        let Some((key, pos)) = candidate else {
            continue;
        };
        match &winner {
            None => winner = Some((key, pos)),
            Some((existing_key, _)) => {
                let cmp = key.as_ref().cmp(existing_key.as_ref());
                let replace = match direction {
                    Direction::Forward => cmp == Ordering::Less,
                    Direction::Backward => cmp == Ordering::Greater,
                };
                // On equal keys we keep the first (highest-priority) entry —
                // fresher levels shadow older ones at the same key.
                if replace {
                    winner = Some((key, pos));
                }
            }
        }
    }
    match winner {
        None => NextEntryResult::NotFound,
        Some((key, pos)) => {
            if pos.is_valid() {
                NextEntryResult::Found(key, pos)
            } else {
                NextEntryResult::SkipDeleted(key)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(bytes: &[u8]) -> Bytes {
        Bytes::from(bytes.to_vec())
    }

    #[test]
    fn merge_all_none() {
        assert_eq!(
            merge_levels_next_entry(vec![None, None, None], Direction::Forward),
            NextEntryResult::NotFound
        );
    }

    #[test]
    fn merge_single_source() {
        let pos = WalPosition::test_value(5);
        let result =
            merge_levels_next_entry(vec![None, Some((k(&[3]), pos)), None], Direction::Forward);
        assert_eq!(result, NextEntryResult::Found(k(&[3]), pos));
    }

    #[test]
    fn merge_two_levels_forward_picks_smaller() {
        let pos = WalPosition::test_value(1);
        // L0 has 5, L1 has 3 — forward picks the smaller (from L1).
        let result = merge_levels_next_entry(
            vec![None, Some((k(&[5]), pos)), Some((k(&[3]), pos))],
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::Found(k(&[3]), pos));
    }

    #[test]
    fn merge_two_levels_backward_picks_larger() {
        let pos = WalPosition::test_value(1);
        let result = merge_levels_next_entry(
            vec![None, Some((k(&[5]), pos)), Some((k(&[3]), pos))],
            Direction::Backward,
        );
        assert_eq!(result, NextEntryResult::Found(k(&[5]), pos));
    }

    #[test]
    fn merge_equal_keys_highest_priority_wins() {
        let p0 = WalPosition::test_value(100);
        let p1 = WalPosition::test_value(200);
        // In-memory (priority 0) and L0 (priority 1) both have key 3 — in-memory wins.
        let result = merge_levels_next_entry(
            vec![Some((k(&[3]), p0)), Some((k(&[3]), p1))],
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::Found(k(&[3]), p0));
    }

    #[test]
    fn merge_in_memory_tombstone_shadows_disk_hit() {
        let valid = WalPosition::test_value(42);
        let tomb = WalPosition::INVALID;
        // In-memory tombstone on key 3 shadows L0's valid 3.
        let result = merge_levels_next_entry(
            vec![Some((k(&[3]), tomb)), Some((k(&[3]), valid))],
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::SkipDeleted(k(&[3])));
    }

    #[test]
    fn merge_l0_tombstone_shadows_l1_hit() {
        let valid = WalPosition::test_value(42);
        let tomb = WalPosition::INVALID;
        // L0 tombstone on key 3 shadows L1's valid 3 (no in-memory).
        let result = merge_levels_next_entry(
            vec![None, Some((k(&[3]), tomb)), Some((k(&[3]), valid))],
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::SkipDeleted(k(&[3])));
    }

    #[test]
    fn merge_disjoint_keys_tombstone_on_larger() {
        let valid = WalPosition::test_value(42);
        let tomb = WalPosition::INVALID;
        // L0 has a tombstone on key 5; L1 has valid key 3. Forward: pick 3.
        // L0's tombstone on 5 does not shadow L1's 3.
        let result = merge_levels_next_entry(
            vec![None, Some((k(&[5]), tomb)), Some((k(&[3]), valid))],
            Direction::Forward,
        );
        assert_eq!(result, NextEntryResult::Found(k(&[3]), valid));
    }
}
