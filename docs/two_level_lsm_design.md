# Two-Level LSM Design

> **Status: design stage.** No code has landed yet. The first implementation
> phase introduces a level-generic in-memory abstraction without changing
> on-disk format or flush behavior; subsequent phases add the L0/L1 split and
> promote logic. Details below may evolve.

## 1. Motivation — write amplification

Today each cell owns a single serialized index blob. Every flush reads the
full on-disk blob, merges in the dirty keys, and rewrites the whole thing
(`flusher::IndexFlusherThread::handle_command` → `merge_dirty_and_clean`).
For a cell with `N` entries and a dirty-flush threshold of `D` keys, the
write amplification per dirty key is:

```
WAF₁ = N / D
```

For large cells this is the dominant source of IO. A cell with 1 M entries
and `D = 1024` rewrites 1 GB of index to durably record 1 MB of new keys —
WAF ≈ 1000×.

## 2. Core idea

Each cell references **a list of on-disk index blobs** instead of exactly one.
The in-memory overlay (`IndexTable`) is unchanged; only the persisted side
grows a level dimension:

```
cell ─▶ [ L0 blob ,  L1 blob ,  … ]
            ^ small, hot,          ^ large, cold,
              rewritten often        rewritten rarely
```

Semantics when multiple levels exist:
- **Union**: the logical cell content is the union of all levels.
- **Overlap**: if a key appears in multiple levels, the **lowest-index level
  wins** (L0 is freshest). This mirrors classical LSM.
- **Promote**: when L0 grows past a size bound, L0 is merged into L1, and L0
  becomes empty. The merged L1 is written once, then L0 resumes accumulating.

Two-level is the default policy. The schema admits more levels; the runtime
state machine maintains exactly two in the first iteration (see §9).

## 3. Write amplification analysis

Let:
- `N` = total entries in a cell
- `D` = dirty-flush threshold (number of keys that trigger an L0 flush)
- `M` = L0 capacity, measured in units of `D` (so L0 holds at most `M·D` entries)

Between two L1 promotes, L0 grows through roughly `D, 2D, …, M·D` entries;
each intermediate L0 rewrite costs the current L0 size. So across `M`
dirty-flush cycles:

- L0 rewrites: `D + 2D + … + M·D = D·M(M+1)/2 ≈ D·M²/2`
- One L1 promote: ≈ `N` bytes (rewrite the full cell)

Total written per `M·D` dirty entries:

```
bytes ≈ D·M²/2 + N
WAF(M) ≈ M/2 + N / (M·D)
```

Minimize over `M`: `dWAF/dM = 1/2 − N/(M²·D) = 0 ⇒ M* = √(2N/D)`, giving

```
WAF* ≈ √(2N / D)
```

Compared to single-level `N/D`:

| N (entries) | WAF single-level | WAF two-level (optimal M) | Reduction |
|------------:|-----------------:|--------------------------:|----------:|
| 16 K        | 16               | ~6                        | ~2.7×     |
| 100 K       | 100              | ~14                       | ~7×       |
| 1 M         | 1 000            | ~45                       | ~22×      |
| 10 M        | 10 000           | ~140                      | ~70×      |

For reference, a generic k-level scheme with size ratio `T = (N/D)^{1/k}`
gives `WAF ≈ k·T`:

| k          | 1     | 2   | 3  | 4  |
|:----------:|:-----:|:---:|:--:|:--:|
| WAF (1M,1k)| 1 000 | ~45 | ~30 | ~28 |

Two levels capture most of the win; going to three only trims a further
~1.5× and adds another promote cascade. The schema is generic, but we
intentionally stop at two in the near-term runtime.

## 4. Proposed schema

### 4.1 `IndexLevels`

New in-memory type (`tidehunter/src/index/levels.rs`):

```rust
/// Ordered list of on-disk index blobs for one cell, freshest first.
///
/// `levels()[0]` is L0 (smallest, most recently flushed), `levels()[1]` is L1,
/// etc. Empty levels (WalPosition::INVALID) are allowed during construction
/// but are pruned before serialization.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IndexLevels {
    // Inlined for the 2-level common case; no heap allocation.
    positions: SmallVec<[WalPosition; 2]>,
}
```

API sketch (level-generic by design):

```rust
impl IndexLevels {
    pub const fn new() -> Self;
    pub fn single(p: WalPosition) -> Self;          // current single-level case
    pub fn is_empty(&self) -> bool;
    pub fn len(&self) -> usize;
    pub fn latest(&self) -> Option<WalPosition>;    // L0 if present
    pub fn iter(&self) -> impl Iterator<Item = WalPosition> + '_;
    pub fn push_front(&mut self, p: WalPosition);   // used on promote
    pub fn replace(&mut self, level: usize, p: WalPosition);
    pub fn truncate(&mut self, len: usize);
}
```

### 4.2 `SnapshotEntryData`

```rust
pub struct SnapshotEntryData {
    pub levels: IndexLevels,          // was: position: WalPosition
    pub last_processed: LastProcessed, // unchanged — single frontier (see §5)
}
```

One `last_processed` per cell is sufficient (see §5); no new tracking fields.

### 4.3 Backward compatibility for on-disk control regions

Existing control regions encode `SnapshotEntryData` as bincode of
`{ position: WalPosition, last_processed: LastProcessed }`. Two options:

- **(A) Custom deserializer for `IndexLevels`.** Detect old layout by
  attempting to decode as `WalPosition` first, and wrap in a single-element
  `IndexLevels` on success; otherwise decode as a `Vec<WalPosition>`. Simple
  but fragile — bincode has no schema discriminator.
- **(B) Explicit format version on `ControlRegion`.** Read a leading
  version byte / `#[serde(default)]` `version: u32` and dispatch to an old
  deserializer. Cleaner; matches the existing `keyspace_names` migration
  pattern in `control.rs`.

We will use **(B)**. Version 1 is the legacy single-`position` layout;
version 2 carries `IndexLevels`. On-disk format upgrades are one-way.

## 5. Why a single `last_processed` suffices

`last_processed` denotes "max WAL offset durably reflected in this cell's
persisted state" — across L0 ∪ L1. L0 is always at least as fresh as L1,
so:
- An L0 flush advances the frontier from `X → Y` (L1 unchanged).
- A promote rewrites L1 to include L0's contents, then empties L0; the
  frontier stays at the pre-promote value.

Either way there is a single monotonic number describing the durable
frontier. Crash recovery only cares about `max(frontier over all levels)`,
not per-level values.

The ControlRegion update must be atomic with respect to any promote, so a
crash mid-promote yields one of:
- **Pre-promote surviving**: old L0 + old L1 on disk, old `levels` and old
  `last_processed` in the control region.
- **Post-promote surviving**: empty L0 + new L1 on disk, new `levels` and
  same `last_processed` in the control region.

Both states are consistent.

## 6. Write path

### 6.1 Flush kinds

`flusher.rs` currently has three `FlushKind`s: `MergeUnloaded`,
`FlushLoaded`, `ForceRelocate`. We add two more (and eventually retire the
old ones or map them onto the new ones):

```rust
pub enum FlushKind {
    // --- new, level-aware ---
    FlushL0 { dirty: Arc<IndexTable>, current_levels: IndexLevels },
    PromoteL0 { current_levels: IndexLevels },

    // --- legacy ---
    MergeUnloaded(WalPosition, Arc<IndexTable>),
    FlushLoaded(Arc<IndexTable>),
    ForceRelocate(WalPosition),
}
```

### 6.2 `FlushL0`

1. If L0 exists, load it; else start with an empty `IndexTable`.
2. Merge-sort (`merge_dirty_and_clean`) with the dirty `IndexTable`.
3. Run compactor / retain-above-position as today.
4. Write the new L0 blob; obtain its `WalPosition`.
5. In the returned `IndexLevels`, replace slot 0 with the new position
   (keep L1 untouched).
6. Update `last_processed` to the current `loader.last_processed_wal_position()`.

This is bounded by L0 size (≈ `M·D`), not `N`.

### 6.3 `PromoteL0`

Triggered when the **post-flush L0 size** exceeds `M·D`:

1. Load L0 and L1.
2. Merge-sort, with L0 winning on overlap.
3. Run compactor (same as today).
4. Write the merged blob as the new L1.
5. Truncate L0 to empty.

This is bounded by `|L0 ∪ L1| ≈ N`, matching the cost of a single legacy
flush. The win is amortization — we pay this cost once per `M` dirty batches.

### 6.4 Triggers

In `large_table.rs::too_many_dirty`:
- If in-memory dirty count exceeds `config.max_dirty_keys` → `FlushL0`.
- If the **resulting** L0 size would exceed `M·D` → also schedule
  `PromoteL0` (as a follow-up command, not inline with the write).

Decision about `M` is a runtime knob (`Config::l0_capacity_multiplier`,
defaulted per `√(2N/D)` heuristic or a safe constant like 8–16).

## 7. Read path

Reads become a loop over levels:

```rust
// Conceptual; real code lives in `large_table::get` / `next_in_cell`.
for position in entry.levels.iter() {
    let reader = loader.index_reader(position)?;
    if let Some(hit) = lookup_in_blob(reader, key)? {
        return Ok(Some(hit));
    }
}
Ok(None)
```

Practical considerations:
- **L0 is small and hot** — in the common case it stays mmap-resident, so
  an extra probe costs one branch, not an IO.
- **Bloom filters per level** are the natural next optimization; today's
  cell-wide bloom filter can be kept on L1 (the slow path) and skipped for
  L0. Out of scope for the initial patch.
- **Iteration** (`next_in_cell`) needs a k-way merge across levels; again,
  with `k = 2` this is a two-way merge, but the code should be written
  against `entry.levels.iter()` so `k ≥ 3` Just Works later.

## 8. Recovery

`ControlRegion` stores `IndexLevels` per cell. WAL replay is unchanged:
replay from `control_region.last_position` forward, overlaying replayed
writes on top of the `IndexTable` loaded from level 0. `unmerge_flushed`
stays a single-frontier operation — it removes in-memory entries with
WAL offset ≤ `last_processed` regardless of which level absorbed them.

One subtlety: after a crash mid-promote we may observe **pre-promote**
state (old L0 + old L1 both populated and overlapping). This is correct
by the overlap rule: L0 wins, so the logical view is identical to the
pre-promote state. The next promote trigger cleans it up.

## 9. Generic-now, two-levels-in-practice

The schema and abstractions are open to `k` levels. The runtime state
machine and flush trigger policy are written against two. Everywhere the
code assumes `levels.len() ≤ 2`, the assumption is marked with
`TODO(levels-generic):` so the boundary is explicit and auditable.

Examples of places where two-level is assumed today (pre-implementation
shortlist):
- `LargeTableEntryState::Unloaded(WalPosition)` carries one position.
  Needs to become either `Unloaded(IndexLevels)` or keep carrying "the
  level we're currently reading from" and let the entry own `IndexLevels`
  alongside.
- `FlushL0` / `PromoteL0` naming — a k-level scheme would want
  `FlushLevel(usize)` / `PromoteLevel(usize)`.
- `too_many_dirty` size comparison — generalizes to per-level size
  thresholds.

These are deferred until the two-level version ships and is measured.

## 10. Implementation phases

The full change is scoped into phases so each lands independently on main:

1. **Abstraction.** Introduce `IndexLevels` type and migrate the
   in-memory consumers of `SnapshotEntryData.position` to go through
   `IndexLevels` helpers. On-disk format unchanged; `IndexLevels` always
   has length 0 or 1. Adds `TODO(levels-generic)` markers. Zero behavior
   change; pure refactor.

2. **On-disk format v2.** Bump control region to version 2; serialize as
   `IndexLevels`. One-way migration on read. Still length 0 or 1 in
   practice.

3. **Flusher split.** Introduce `FlushL0` and `PromoteL0` flush kinds and
   wire triggers. Default `M` chosen conservatively. Add metrics
   (`l0_bytes_written`, `l1_bytes_written`, `promote_total`).

4. **Policy tuning & bloom-per-level.** Measure WAF on realistic
   workloads (Walrus event stream, Sui checkpoint ingest) and tune `M`;
   optionally add a level-1 bloom filter.

This document covers all four phases; phase 1 is tracked by the initial
landing patch.

## 11. Open questions

- **Blob placement.** L0 rewrites are frequent and small. Do we colocate
  L0 blobs with L1 in the existing index WAL region (risks hot-spotting
  the tail) or give L0 a dedicated region for better relocation
  semantics? First pass: reuse the existing region and rely on the
  current relocation path; revisit after measurement.
- **Interaction with `relocation`.** `ForceRelocate` today relocates a
  single blob. With two blobs, relocation has to iterate levels. The
  relocation cutoff logic (`retain_above_position`) is per-blob already,
  so the change is mostly mechanical.
- **`LargeTableEntryState`.** Should `Unloaded(WalPosition)` become
  `Unloaded(IndexLevels)`, or should the state carry only the current
  read level and the entry carry the full list? The second is simpler
  but forces all read paths through the entry, not the state. To be
  decided in phase 3.
- **Small cells.** Below some size threshold, a promote is cheaper than
  the bookkeeping overhead of maintaining two levels. A policy of
  "collapse to single level when `N < κ·D`" is probably worthwhile but
  adds a state transition. Deferred to phase 4.

## 12. References

- Current flush path: `tidehunter/src/flusher.rs:181–234`
- Current merge: `tidehunter/src/index/index_table/mod.rs:948` (`promote_to_flat`),
  and the `merge_dirty_and_clean` / `unmerge_flushed` pair in the same file.
- Snapshot mechanism: `docs/snapshot_mechanism.md` (two-pass snapshot
  protocol; directly affected by this change's definition of "persisted
  frontier").
- Control region: `tidehunter/src/control.rs` (file format, migration
  hooks).
