# wal_inspector → tideconsole Migration Plan

Goal: move all wal_inspector functionality into tideconsole so wal_inspector can be deleted.

---

## Category 1: Necessary and Easy

Direct ports — logic already exists in wal_inspector, just needs wiring into tideconsole.

### 1. `db.force_snapshot()`
Calls `force_rebuild_control_region()` on the live DB (triggers WAL replay if not already open).
Returns the snapshot WAL position as `i64`.

```rhai
let pos = db.force_snapshot();
print("snapshot at " + pos);
```

**Status: done**

---

### 2. `payload_len` in `load_index()` results
`load_index(offset)` currently returns `{ key, wal_position }` per entry.
`wal_position.payload_len()` is available but was dropped. Add it to the map.

```rhai
let entries = db.load_index(offset);
entries[0].payload_len   // bytes of the stored value
```

**Status: done**

---

### 3. `db.verify()`
Integrity check: walks WAL to collect live keys (records minus removes), opens DB,
verifies every key is accessible via `get()` and `scan()`.
Returns a map with counts and an `ok` boolean.

```rhai
let r = db.verify();
// { total: 1000, verified: 1000, missing: 0, errors: 0,
//   iter_verified: 1000, iter_missing: 0, iter_errors: 0, ok: true }
if !r.ok { print("INTEGRITY FAILURE"); }
```

**Status: done**

---

## Category 2: Additional Stats

New computation logic — not a straight port.

### 4. Richer `wal_stats()` / `db.wal_stat_detail()`
Add to per-entry-type breakdown:
- Bytes consumed per type
- Average entry size
- Percentage of total WAL space

Add global key/value size stats:
- Min / max / avg key size in bytes
- Min / max / avg value size in bytes

### 5. CR analysis extras (`db.cr_stats()` or additions to `load_cr()`)
- Index WAL total file size vs. positions size → usage %
- WAL position percentiles P50 / P90 / P99 (via `snapshot.pct_wal_position()`)
- Lowest N cells sorted by position (identifies replay lag hotspots)

### 6. `db.analyze_ks(ks)`
Per-keyspace payload size distribution using the on-disk index:
- Iterates all cells for the keyspace from CR
- Reads each index entry from the index WAL
- Sums `payload_len` for all entries
- Returns distribution: P10–P99, min, max, avg, total

```rhai
let s = db.analyze_ks("objects");
// { total_keys: 500000, total_bytes: 4294967296,
//   p50: 8192, p90: 65536, p99: 1048576, min: 16, max: 2097152 }
```
