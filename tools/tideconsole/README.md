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
```

## Available functions

| Function | Description |
|---|---|
| `open(path)` | Open a TideHunter database directory |
| `walk_wal(visitor)` | Walk WAL from the start, calling `visitor(entry)` for each entry |
| `walk_wal(start_pos, visitor)` | Walk WAL from `start_pos` byte offset |
| `list_wal_files()` | List WAL files with `start_pos`, `size`, and `created` timestamp |
| `wal_stats()` | Print entry-type counts and per-keyspace breakdown |
| `help()` | Show this reference inside the shell |

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
