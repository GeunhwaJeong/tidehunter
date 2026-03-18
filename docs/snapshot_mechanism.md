
# Snapshot Mechanism

A snapshot captures the current state of all index entries in the large table and writes
it to the control region on disk. On the next restart, Tidehunter reads the control region
and replays only the WAL from `replay_from` onward rather than from the very beginning.

## Entry States

Each `LargeTableEntry` can be in one of five states:

| State | Meaning |
|---|---|
| `Empty` | No data for this cell |
| `Unloaded(pos)` | Clean; index is on disk at `pos`, not in memory |
| `Loaded(pos)` | Clean; index is on disk at `pos` and also in memory |
| `DirtyUnloaded(pos)` | Dirty overlay in memory on top of clean disk index at `pos` |
| `DirtyLoaded(pos)` | Dirty overlay in memory on top of clean disk index at `pos` (also loaded) |

`pos` is always the on-disk WAL position of the last flushed clean index. Dirty entries
have unsaved in-memory writes on top of it.

Each entry also carries `last_processed: LastProcessed` — the WAL position through which
this entry's **on-disk index** is known to be complete. Writes at or after `last_processed`
exist only in the in-memory dirty overlay and the WAL; the on-disk index at `pos` does not
reflect them. This is updated:

- When an async flush completes (`update_flushed_index` / `update_relocated_position`):
  set to the `loader.last_processed_wal_position()` that was captured when the flush
  was enqueued.
- During snapshot pass 2 for clean entries: advanced to the current
  `loader.last_processed_wal_position()`.

## Two-Pass Snapshot

`rebuild_control_region_from(threshold_position)` runs in two passes:

### Pass 1 — Queue flushes (no IO under cell locks)

`prefetch_flushes_for_snapshot` iterates all entries under their row locks but performs
no IO. For each entry it calls `request_async_snapshot_flush`, which:

1. Skips the entry if a flush is already in-flight (`pending_last_processed.is_some()`).
2. **Dirty entries with `last_processed <= threshold_position`**: queues an async flush
   (`FlushKind::MergeUnloaded` or `FlushKind::FlushLoaded`) via the flusher. Sets
   `pending_last_processed = Some(loader.last_processed_wal_position())`.
3. **Clean entries with on-disk index position `pos < force_relocate_below`**: queues
   `FlushKind::ForceRelocate(pos)`. The flusher loads the index at that index WAL position
   and re-writes it to a newer index WAL position, without reading any in-memory state.
   Sets `pending_last_processed` the same way.
4. All other entries (clean within range, or dirty above threshold): skipped.

The row lock is released immediately after queuing — IO happens off the critical path.

> **Note:** the async flush path always evicts entries from memory after flushing (equivalent
> to `unload=true`). The previous synchronous snapshot path used `unload=false`, preserving
> in-memory state after the flush. The practical impact is limited because only entries with
> `last_processed <= threshold_position` are flushed here — entries that have not been written
> to for at least `snapshot_unload_threshold` bytes of WAL and are therefore unlikely to be hot.

### Barrier

`self.large_table.flusher.barrier()` blocks until all previously enqueued flusher messages
have been processed (including their `update_flushed_index` / `update_relocated_position`
callbacks). By the time `barrier()` returns, every entry that was queued in pass 1 is
clean and its on-disk position is up to date.

The barrier works by appending a `FlusherCommand::Barrier(Arc<SendGuard>)` to every
flusher thread's queue. `SendGuard` holds an `ArcMutexGuard`. The caller drops its own
`Arc<SendGuard>` and then blocks on `mutex.lock()`. Each flusher thread drops its clone
of the `Arc` after processing all preceding commands, releasing the mutex. Because all
threads receive the `Barrier` in a tight loop under a single `Arc`, they can process it
in parallel.

### Pass 2 — Take snapshot (read-only, minimal IO under cell locks)

`snapshot()` iterates all entries again, briefly locking each row:

- Calls `entry.promote_pending()` to flush any committed-but-not-yet-visible batch writes
  into the entry's dirty index. This is pure in-memory work.
- For clean non-empty entries, advances `entry.last_processed` to
  `loader.last_processed_wal_position()`. This keeps `replay_from` tight (see below).
- Records `(position, last_processed)` for each non-empty entry as `SnapshotEntryData`.

## `replay_from` Calculation

`replay_from` is the WAL offset from which Tidehunter must replay on restart to
reconstruct all data not reflected in the snapshot's on-disk indices.

For each keyspace, `ks_snapshot` computes:
- `replay_from`: the **minimum `last_processed`** across all non-empty entries. Any entry
  with a lower `last_processed` has writes that may not be in its on-disk index yet;
  those writes exist only in the WAL and must be replayed.
- `max_wal_position`: the **maximum on-disk index position** across all entries. Used only
  when all entries are clean (no dirty `last_processed` to use as floor).

The final `replay_from` for the snapshot is determined as:

1. **Non-empty database** → `min(last_processed)` across all non-empty entries in all
   keyspaces. Every non-empty entry contributes its `last_processed` regardless of whether
   it is dirty or clean (clean entries have `last_processed` advanced to the WAL frontier
   in pass 2, so they do not pull `replay_from` back unnecessarily).
2. **Empty database** → `loader.last_processed_wal_position()` captured at the start of
   `snapshot()`, before iterating any entries. Capturing it early ensures that writes
   arriving during iteration are not silently skipped on replay.

## Why Updates Are Not Lost

Every write appended to the WAL is assigned to exactly one cell. Before a snapshot is
stored to the control region, the following chain holds:

1. **Dirty entries that are below `threshold_position`** are flushed to disk in pass 1.
   Their on-disk index now contains those writes.
2. **Dirty entries above `threshold_position`** are skipped by pass 1. They remain dirty
   at snapshot time. Their `last_processed` is the lowest WAL position at which their
   in-memory dirty overlay diverged from their on-disk index, so `replay_from` is pulled
   back to at least that position.
3. **Clean entries** have `last_processed` advanced to the current WAL frontier in pass 2.
   Any WAL writes for them are already reflected on disk and do not require replay.
4. **Entries with an in-flight async flush at pass 1 time** are skipped by
   `request_async_snapshot_flush` (the `pending_last_processed.is_some()` guard). Their
   already-queued flush completes before the barrier returns, so by pass 2 they are clean
   and their `last_processed` is updated.

After a crash and restart, Tidehunter loads the control region and replays the WAL from
`replay_from`. Every write that was not yet reflected in any on-disk index at snapshot time
is guaranteed to lie at or after `replay_from`, so replaying from that position is both
sufficient (no data loss) and complete (no duplicate writes, since WAL replay uses
`last_processed` per entry to skip entries already covered by the index).

## Why `replay_from` Is Bounded by `snapshot_unload_threshold`

```
threshold_position = current_wal_position - snapshot_unload_threshold
```

Pass 1 only flushes dirty entries whose `last_processed <= threshold_position`. That means
any dirty entry whose `last_processed` is within `snapshot_unload_threshold` bytes of the
WAL tail is intentionally left dirty.

However, `replay_from` is the **minimum** `last_processed` across all non-empty entries.
Clean entries have `last_processed` advanced to the current WAL frontier (pass 2), and
dirty entries that were flushed in pass 1 also have their `last_processed` advanced to
near the frontier (set when the flush was enqueued). The only entries that can pull
`replay_from` backward are dirty entries that were skipped in pass 1 — i.e., those with
`last_processed > threshold_position`.

Therefore:

```
replay_from >= threshold_position
            = current_wal_position - snapshot_unload_threshold
```

The distance between the WAL tail and `replay_from` is at most `snapshot_unload_threshold`.
On restart, only that bounded window of WAL data needs to be replayed. Entries that were
above the threshold remain dirty (or get flushed by normal background unloading before the
next snapshot), but they never push `replay_from` below the threshold.

The default value is 64 GiB for production and 256 MiB for tests. Setting it smaller
reduces replay time at the cost of more aggressive flushing during snapshot.

## Forced Relocation

When WAL compaction (relocation) is in progress, old WAL segments cannot be freed until no
on-disk index file references a position within them. `force_relocate_below` is the lowest
**index** WAL position that must be vacated — it refers to where index data is stored in the
WAL, not to value (key-value record) positions which are tracked separately throughout the
rest of this document.

Clean entries with `pos < force_relocate_below` are handled by `FlushKind::ForceRelocate(pos)`:
the flusher loads the index at the old index position, re-writes it to a new index WAL
position, and calls `update_relocated_position` to update the entry's state to the new
position. The entry's dirty overlay (if a write arrived during the in-flight relocation) is
preserved untouched; only the base index position changes.

Dirty entries with `pos < force_relocate_below` do not need special treatment: their normal
dirty flush (queued in pass 1) will write the merged index to a new index WAL position that
is by definition `>= force_relocate_below`, satisfying the relocation constraint.

## `pending_last_processed` Invariant

`pending_last_processed: Option<LastProcessed>` serializes flushes:

- `None`: no flush is in-flight; reads and writes may proceed freely.
- `Some(lp)`: exactly one flush is in-flight. `lp` is the value that will be assigned to
  `last_processed` when the flush completes.

While `pending_last_processed` is `Some`:
- New async flushes cannot be queued (`unload_if_ks_enabled` and
  `request_async_snapshot_flush` both guard on `is_some()`).
- `sync_flush` panics if called (`assert!(pending_last_processed.is_none())`).
- Writes can still arrive and make the entry dirty; they accumulate in `pending_data` and
  are promoted to the dirty index on the next `promote_pending` call.
- `remove_entry` panics if called (asserts `is_none()`).

`update_flushed_index` and `update_relocated_position` both call
`pending_last_processed.take().expect(...)` under the row lock, atomically clearing the
flag and updating `last_processed`. This ensures no second flush can be queued until the
first has fully committed its result.
