# tideconsole

An interactive [Rhai](https://rhai.rs/) shell for inspecting TideHunter databases.
Rhai has Rust-like syntax, so visitor closures feel natural to Rust developers.

## Build

```bash
cargo build -p tideconsole --release
```

## Usage

```bash
# Start with a database pre-opened
tideconsole --db /path/to/db

# Start without a database (call open() inside the shell)
tideconsole

# Run a one-liner and exit (no REPL)
tideconsole --db /path/to/db --exec 'wal_stats()'

# Run a script file and exit
tideconsole --db /path/to/db --script analysis.rhai
```

## Available functions

### Database management

| Function | Description |
|---|---|
| `open(path)` | Open a TideHunter database directory |
| `help()` | Show this reference inside the shell |

### Query functions

| Function | Description |
|---|---|
| `get(ks, key_hex)` | Look up a key; returns value as hex string, or `()` if not found |
| `exists(ks, key_hex)` | Returns `true` if the key exists |
| `scan(ks, visitor)` | Iterate all live keys; calls `visitor(key_hex, value_hex)` |
| `scan(ks, lower_hex, visitor)` | Iterate from `lower_hex` bound (inclusive) |
| `scan(ks, lower_hex, upper_hex, visitor)` | Iterate between bounds (upper exclusive) |

### Manipulation functions

| Function | Description |
|---|---|
| `put(ks, key_hex, value_hex)` | Write a key-value record |
| `delete(ks, key_hex)` | Delete a key |

`ks` accepts either an integer keyspace ID (`0`) or a keyspace name string (`"objects"`).

### WAL inspection

| Function | Description |
|---|---|
| `walk_wal(visitor)` | Walk WAL from the start, calling `visitor(entry)` for each entry |
| `walk_wal(start_pos, visitor)` | Walk WAL from `start_pos` byte offset |
| `list_wal_files()` | List WAL files with `start_pos`, `size`, and `created` timestamp |
| `wal_stats()` | Print entry-type counts and per-keyspace breakdown |

### Control region inspection

These functions read the `cr` file directly — no WAL replay or `Db::open` required.

| Function | Description |
|---|---|
| `load_cr()` | Load the full control region into a Rhai map for scripted inspection |

`load_cr()` returns:
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
          "offset":         i64,           // index WAL offset; -1 if no index yet
          "len":            i64,           // index frame length; 0 if no index yet
          "last_processed": i64,           // WAL offset up to which the index is current
        },
        ...
      ]
    },
    ...
  ]
}
```

## Entry fields

Inside a `walk_wal` visitor the `entry` object exposes:

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
let cr = load_cr();
print("WAL replay starts at: " + cr.last_position);
for ks in cr.keyspaces {
    let valid = 0;
    for c in ks.cells {
        if c.offset >= 0 { valid += 1; }
    }
    print(ks.name + ": " + ks.cells.len() + " cells, " + valid + " with index");
}
```

**Look up a single key:**
```rhai
let v = get("objects", "deadbeef00000001");
if v == () {
    print("not found");
} else {
    print("value: " + v);
}
```

**Count live records in a keyspace:**
```rhai
let count = 0;
scan("objects", |key, value| { count += 1; });
print("live records: " + count);
```

**Find keys with large values:**
```rhai
scan("objects", |key, value| {
    if value.len() > 64 {
        print(key + " -> " + value.len() + " hex chars");
    }
});
```

**Range scan between two keys:**
```rhai
let start = "0000000000000001";
let end   = "0000000000001000";
scan("objects", start, end, |key, value| {
    print(key);
});
```

**Write and verify a record:**
```rhai
put("objects", "cafebabe00000001", "0102030405060708");
print(exists("objects", "cafebabe00000001"));   // true
print(get("objects", "cafebabe00000001"));      // 0102030405060708
delete("objects", "cafebabe00000001");
print(exists("objects", "cafebabe00000001"));   // false
```

**Count records per keyspace:**
```rhai
let counts = #{};
walk_wal(|entry| {
    if entry.entry_type == "record" {
        let k = entry.keyspace.to_string();
        counts[k] = if counts.contains(k) { counts[k] + 1 } else { 1 };
    }
});
counts
```

**Find the largest values:**
```rhai
let max_len = 0;
let max_key = "";
walk_wal(|entry| {
    if entry.entry_type == "record" && entry.value_len > max_len {
        max_len = entry.value_len;
        max_key = entry.key;
    }
});
print("largest value: " + max_len + " bytes at key " + max_key);
```

**Print all removes for a specific keyspace:**
```rhai
walk_wal(|entry| {
    if entry.entry_type == "remove" && entry.keyspace == 3 {
        print(entry.key + " @ offset " + entry.position);
    }
});
```

**List WAL files then walk only the most recent one:**
```rhai
let files = list_wal_files();
print("WAL files: " + files.len());
for f in files {
    print(f.name + "  start=" + f.start_pos + "  size=" + f.size);
}

// Walk entries only in the last file
let last = files[files.len() - 1];
walk_wal(last.start_pos, |entry| {
    print(entry.entry_type + " ks=" + entry.keyspace);
});
```

**Compute write amplification — index bytes vs record bytes:**
```rhai
let record_bytes = 0;
let index_bytes = 0;
walk_wal(|entry| {
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
let target = "deadbeefcafe0001";
walk_wal(|entry| {
    if entry.key == target {
        print(entry.entry_type + " @ " + entry.position);
    }
});
```

**Count entries written after a known WAL offset (e.g. after a checkpoint):**
```rhai
let checkpoint_pos = 0x1000000;
let count = 0;
walk_wal(checkpoint_pos, |entry| {
    if entry.entry_type == "record" { count += 1; }
});
print("records since checkpoint: " + count);
```

## Multi-line input

The shell detects unbalanced braces and waits for more input before evaluating,
so you can paste or type multi-line closures freely. Use Ctrl+C to discard
incomplete input and Ctrl+D to exit.

History is saved to `~/.tideconsole_history`.
