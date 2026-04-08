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
| `walk_wal(visitor)` | Walk every WAL entry, calling `visitor(entry)` for each |
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

Count records per keyspace:
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

Find the largest values:
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

Print WAL positions of all removes for a specific keyspace:
```rhai
walk_wal(|entry| {
    if entry.entry_type == "remove" && entry.keyspace == 3 {
        print(entry.key + " @ offset " + entry.position);
    }
});
```

## Multi-line input

The shell detects unbalanced braces and waits for more input before evaluating,
so you can paste or type multi-line closures freely. Use Ctrl+C to discard
incomplete input and Ctrl+D to exit.

History is saved to `~/.tideconsole_history`.
