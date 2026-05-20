# tideconsole

An interactive [Rhai](https://rhai.rs/) shell for inspecting TideHunter databases.
Rhai has Rust-like syntax, so visitor closures feel natural to Rust developers.

## Build

```bash
cargo build -p tideconsole --release
```

## Usage

```bash
# Start with a database pre-opened as 'db'
tideconsole --db /path/to/db

# Start without a database (call open() inside the shell)
tideconsole

# Run a one-liner and exit (no REPL)
tideconsole --db /path/to/db --exec 'db.wal_stats()'

# Run a script file and exit
tideconsole --db /path/to/db --script analysis.rhai
```

## API overview

Databases are opened with `open()`, which returns a handle. All operations are
methods on that handle. Multiple databases can be open at the same time.

```rhai
let db = open("/path/to/db");
db.wal_stats();

let db2 = open("/path/to/other/db");
db2.scan("objects", |k, v| { print(k); });
```

When `--db /path` is passed on the command line, the handle is automatically
bound to the variable `db`.

## Available functions

### Opening databases

| Function | Description |
|---|---|
| `let db = open(path)` | Open a TideHunter database; returns a db handle |
| `help()` | Show this reference inside the shell |

### Query methods

| Method | Description |
|---|---|
| `db.get(ks, key_hex)` | Look up a key; returns value as hex string, or `()` if not found |
| `db.exists(ks, key_hex)` | Returns `true` if the key exists |
| `db.scan(ks, visitor)` | Iterate all live keys; calls `visitor(key_hex, value_hex)` |
| `db.scan(ks, lower_hex, visitor)` | Iterate from `lower_hex` bound (inclusive) |
| `db.scan(ks, lower_hex, upper_hex, visitor)` | Iterate between bounds (upper exclusive) |

### Manipulation methods

| Method | Description |
|---|---|
| `db.put(ks, key_hex, value_hex)` | Write a key-value record |
| `db.delete(ks, key_hex)` | Delete a key |

`ks` accepts either an integer keyspace ID (`0`) or a keyspace name string (`"objects"`).

### WAL inspection methods

| Method | Description |
|---|---|
| `db.walk_wal(visitor)` | Walk WAL from the start, calling `visitor(entry)` for each entry |
| `db.walk_wal(start_pos, visitor)` | Walk WAL from `start_pos` byte offset |
| `db.list_wal_files()` | List WAL files with `start_pos`, `size`, and `created` timestamp |
| `db.wal_stats()` | Print entry-type counts and per-keyspace breakdown |

### Control region inspection methods

These methods read the `cr` file directly — no WAL replay or `Db::open` required.

| Method | Description |
|---|---|
| `db.load_cr()` | Load the full control region into a Rhai map for scripted inspection |
| `db.load_index(offset)` | Read the on-disk index at a given index WAL offset |

`db.load_cr()` returns:
```
{
  "last_position": i64,       // WAL offset at which replay stopped
  "keyspaces": [              // one entry per keyspace, in keyspace-ID order
    {
      "name":  string,
      "id":    i64,
      "cells": [              // every cell in the snapshot
        {
          "cell_id":        i64 | string,  // integer for Uniform KS, hex for PrefixedUniform
          "levels": [                      // one entry per populated on-disk blob,
            {                              //   freshest (L0) first; empty if no index yet
              "level":  i64,               // schema slot (0 = L0, 1 = L1, …); shard
                                           //   blobs are reported at L1 too
              "offset": i64,               // index WAL offset
              "len":    i64,               // index frame length
            },
            ...
          ],
          "shards": [                      // empty unless auto_sharding has split L1
            {                              //   into per-range sub-blobs
              "min_key": string,           // hex; shard's smallest content key
              "max_key": string,           // hex; shard's largest content key
              "offset":  i64,              // index WAL offset (same as the matching
                                           //   "level":1 entry under `levels`)
              "len":     i64,              // index frame length
            },
            ...
          ],
          "last_processed": i64,           // WAL offset up to which the index is current
        },
        ...
      ]
    },
    ...
  ]
}
```

`db.load_index(offset)` returns:
```
[ { "key": "<hex>", "wal_position": <i64> }, ... ]
```

## Entry fields

Inside a `db.walk_wal` visitor the `entry` object exposes:

| Field | Type | Description |
|---|---|---|
| `entry.entry_type` | string | `"record"`, `"remove"`, `"index"`, `"batch_start"`, `"drop_cells"` |
| `entry.keyspace` | int | Keyspace ID |
| `entry.key` | string | Key bytes as a lowercase hex string |
| `entry.value` | string | Value bytes as a lowercase hex string (records only) |
| `entry.value_len` | int | Value length in bytes |
| `entry.position` | int | Byte offset of the frame in the WAL |
| `entry.raw_size` | int | Total frame size including CRC header |

## Examples

**Inspect the control region:**
```rhai
let db = open("/data/mydb");
let cr = db.load_cr();
print("WAL replay starts at: " + cr.last_position);
for ks in cr.keyspaces {
    let valid = 0;
    for c in ks.cells {
        if c.levels.len() > 0 { valid += 1; }
    }
    print(ks.name + ": " + ks.cells.len() + " cells, " + valid + " with index");
}
```

**Count cells that have been promoted to L1:**
```rhai
let db = open("/data/mydb");
let cr = db.load_cr();
for ks in cr.keyspaces {
    let promoted = 0;
    for c in ks.cells {
        for lvl in c.levels {
            if lvl.level >= 1 { promoted += 1; break; }
        }
    }
    print(ks.name + ": " + promoted + " cells with L1");
}
```

**Find sharded cells and their shard ranges:**
```rhai
let db = open("/data/mydb");
let cr = db.load_cr();
for ks in cr.keyspaces {
    for c in ks.cells {
        if c.shards.len() > 0 {
            print(ks.name + " cell " + c.cell_id + ": " + c.shards.len() + " shards");
            for s in c.shards {
                print("  [" + s.min_key + " .. " + s.max_key + "]  " + s.len + " bytes");
            }
        }
    }
}
```

**Look up a single key:**
```rhai
let db = open("/data/mydb");
let v = db.get("objects", "deadbeef00000001");
if v == () {
    print("not found");
} else {
    print("value: " + v);
}
```

**Count live records in a keyspace:**
```rhai
let db = open("/data/mydb");
let count = 0;
db.scan("objects", |key, value| { count += 1; });
print("live records: " + count);
```

**Find keys with large values:**
```rhai
let db = open("/data/mydb");
db.scan("objects", |key, value| {
    if value.len() > 64 {
        print(key + " -> " + value.len() + " hex chars");
    }
});
```

**Range scan between two keys:**
```rhai
let db = open("/data/mydb");
let start = "0000000000000001";
let end   = "0000000000001000";
db.scan("objects", start, end, |key, value| {
    print(key);
});
```

**Write and verify a record:**
```rhai
let db = open("/data/mydb");
db.put("objects", "cafebabe00000001", "0102030405060708");
print(db.exists("objects", "cafebabe00000001"));   // true
print(db.get("objects", "cafebabe00000001"));      // 0102030405060708
db.delete("objects", "cafebabe00000001");
print(db.exists("objects", "cafebabe00000001"));   // false
```

**Count records per keyspace:**
```rhai
let db = open("/data/mydb");
let counts = #{};
db.walk_wal(|entry| {
    if entry.entry_type == "record" {
        let k = entry.keyspace.to_string();
        counts[k] = if counts.contains(k) { counts[k] + 1 } else { 1 };
    }
});
counts
```

**Find the largest values:**
```rhai
let db = open("/data/mydb");
let max_len = 0;
let max_key = "";
db.walk_wal(|entry| {
    if entry.entry_type == "record" && entry.value_len > max_len {
        max_len = entry.value_len;
        max_key = entry.key;
    }
});
print("largest value: " + max_len + " bytes at key " + max_key);
```

**Print all removes for a specific keyspace:**
```rhai
let db = open("/data/mydb");
db.walk_wal(|entry| {
    if entry.entry_type == "remove" && entry.keyspace == 3 {
        print(entry.key + " @ offset " + entry.position);
    }
});
```

**List WAL files then walk only the most recent one:**
```rhai
let db = open("/data/mydb");
let files = db.list_wal_files();
print("WAL files: " + files.len());
for f in files {
    print(f.name + "  start=" + f.start_pos + "  size=" + f.size);
}

// Walk entries only in the last file
let last = files[files.len() - 1];
db.walk_wal(last.start_pos, |entry| {
    print(entry.entry_type + " ks=" + entry.keyspace);
});
```

**Compute write amplification — index bytes vs record bytes:**
```rhai
let db = open("/data/mydb");
let record_bytes = 0;
let index_bytes = 0;
db.walk_wal(|entry| {
    if entry.entry_type == "record" { record_bytes += entry.raw_size; }
    if entry.entry_type == "index"  { index_bytes  += entry.raw_size; }
});
print("record bytes : " + record_bytes);
print("index bytes  : " + index_bytes);
if record_bytes > 0 {
    print("amplification: " + (index_bytes * 100 / record_bytes) + "%");
}
```

**Scan a WAL range for a specific key (hex):**
```rhai
let db = open("/data/mydb");
let target = "deadbeefcafe0001";
db.walk_wal(|entry| {
    if entry.key == target {
        print(entry.entry_type + " @ " + entry.position);
    }
});
```

**Count entries written after a known WAL offset (e.g. after a checkpoint):**
```rhai
let db = open("/data/mydb");
let checkpoint_pos = 0x1000000;
let count = 0;
db.walk_wal(checkpoint_pos, |entry| {
    if entry.entry_type == "record" { count += 1; }
});
print("records since checkpoint: " + count);
```

**Compare two databases side by side:**
```rhai
let db1 = open("/data/db1");
let db2 = open("/data/db2");
db1.wal_stats();
db2.wal_stats();
```

## Multi-line input

The shell detects unbalanced braces and waits for more input before evaluating,
so you can paste or type multi-line closures freely. Use Ctrl+C to discard
incomplete input and Ctrl+D to exit.

History is saved to `~/.tideconsole_history`.
