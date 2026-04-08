use parking_lot::Mutex;
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, NativeCallContext, Scope};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tidehunter::WalKind;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::KeyShape;
use tidehunter::test_utils::{Metrics, Wal, WalEntry, WalError};

// ---------------------------------------------------------------------------
// Shared console state
// ---------------------------------------------------------------------------

pub struct ConsoleContext {
    pub db_path: Option<PathBuf>,
    pub config: Config,
    pub key_shape: Option<KeyShape>,
    /// Output sink used by all registered functions (open, wal_stats, etc.).
    /// Defaults to `println!`; override in tests to capture output.
    pub print_fn: Arc<dyn Fn(&str) + Send + Sync>,
}

impl Default for ConsoleContext {
    fn default() -> Self {
        Self {
            db_path: None,
            config: Config::default(),
            key_shape: None,
            print_fn: Arc::new(|s| println!("{s}")),
        }
    }
}

// ---------------------------------------------------------------------------
// Entry — the Rhai-visible WAL entry type
// ---------------------------------------------------------------------------

/// Represents a single WAL entry exposed to Rhai scripts.
#[derive(Clone, Debug)]
pub struct Entry {
    /// "record" | "remove" | "index" | "batch_start" | "drop_cells"
    pub entry_type: String,
    /// Keyspace ID (u8 widened to i64 for Rhai)
    pub keyspace: i64,
    /// Key bytes as a lowercase hex string
    pub key: String,
    /// Value bytes as a lowercase hex string (empty for non-records)
    pub value: String,
    /// Value length in bytes
    pub value_len: i64,
    /// Byte offset of this frame in the WAL
    pub position: i64,
    /// Total frame size (including CRC header) in bytes
    pub raw_size: i64,
}

impl std::fmt::Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Entry {{ type: {}, ks: {}, key: {}, value_len: {}, pos: {:#x} }}",
            self.entry_type, self.keyspace, self.key, self.value_len, self.position
        )
    }
}

fn from_wal_entry(entry: WalEntry, offset: u64, raw_size: usize) -> Entry {
    let position = offset as i64;
    let raw_size = raw_size as i64;
    match entry {
        WalEntry::Record(ks, key, value, _relocated) => Entry {
            entry_type: "record".into(),
            keyspace: ks.as_u8() as i64,
            key: hex::encode(&key),
            value_len: value.len() as i64,
            value: hex::encode(&value),
            position,
            raw_size,
        },
        WalEntry::Remove(ks, key) => Entry {
            entry_type: "remove".into(),
            keyspace: ks.as_u8() as i64,
            key: hex::encode(&key),
            value: String::new(),
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::Index(ks, _data) => Entry {
            entry_type: "index".into(),
            keyspace: ks.as_u8() as i64,
            key: String::new(),
            value: String::new(),
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::BatchStart(size) => Entry {
            entry_type: "batch_start".into(),
            keyspace: 0,
            key: String::new(),
            value: String::new(),
            value_len: size as i64,
            position,
            raw_size,
        },
        WalEntry::DropCells(ks, _from, _to) => Entry {
            entry_type: "drop_cells".into(),
            keyspace: ks.as_u8() as i64,
            key: String::new(),
            value: String::new(),
            value_len: 0,
            position,
            raw_size,
        },
    }
}

// ---------------------------------------------------------------------------
// WAL walking helper (shared by walk_wal and wal_stats)
// ---------------------------------------------------------------------------

fn do_walk_wal<F>(
    db_path: &Path,
    config: &Config,
    start_pos: u64,
    mut f: F,
) -> Result<(), Box<EvalAltResult>>
where
    F: FnMut(Entry) -> Result<(), Box<EvalAltResult>>,
{
    let metrics = Metrics::new();
    let wal = Wal::open(db_path, config.wal_layout(WalKind::Replay), metrics)
        .map_err(|e| -> Box<EvalAltResult> { format!("Failed to open WAL: {e:?}").into() })?;

    let mut iter = wal
        .wal_iterator(start_pos)
        .map_err(|e| -> Box<EvalAltResult> {
            format!("Failed to create WAL iterator: {e:?}").into()
        })?;

    loop {
        match iter.next() {
            Ok((position, raw_bytes)) => {
                let raw_size = raw_bytes.len();
                let offset = position.offset();
                let entry = from_wal_entry(WalEntry::from_bytes(raw_bytes), offset, raw_size);
                f(entry)?;
            }
            // CRC errors and EndOfWal both signal the end of written data
            Err(WalError::Crc(_)) | Err(WalError::EndOfWal) => break,
            Err(WalError::Io(e)) => {
                return Err(format!("WAL I/O error: {e}").into());
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Engine factory
// ---------------------------------------------------------------------------

pub fn create_engine(ctx: Arc<Mutex<ConsoleContext>>) -> Engine {
    let mut engine = Engine::new();

    // --- Register Entry type ---
    engine.register_type_with_name::<Entry>("Entry");
    engine.register_get("entry_type", |e: &mut Entry| e.entry_type.clone());
    engine.register_get("keyspace", |e: &mut Entry| e.keyspace);
    engine.register_get("key", |e: &mut Entry| e.key.clone());
    engine.register_get("value", |e: &mut Entry| e.value.clone());
    engine.register_get("value_len", |e: &mut Entry| e.value_len);
    engine.register_get("position", |e: &mut Entry| e.position);
    engine.register_get("raw_size", |e: &mut Entry| e.raw_size);
    engine.register_fn("to_string", |e: &mut Entry| e.to_string());
    engine.register_fn("to_debug", |e: &mut Entry| format!("{e:?}"));

    // --- open(path) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "open",
            move |path: &str| -> Result<(), Box<EvalAltResult>> {
                let db_path = PathBuf::from(path);
                let config = Db::load_config(&db_path).unwrap_or_default();
                let key_shape =
                    Db::load_key_shape(&db_path).map_err(|e| -> Box<EvalAltResult> {
                        format!("Failed to load key shape: {e:?}").into()
                    })?;
                let mut ctx = ctx.lock();
                ctx.db_path = Some(db_path);
                ctx.config = config;
                ctx.key_shape = Some(key_shape);
                (ctx.print_fn)(&format!("Opened database at {path}"));
                Ok(())
            },
        );
    }

    // --- walk_wal(|entry| { ... }) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "walk_wal",
            move |native_ctx: NativeCallContext,
                  visitor: FnPtr|
                  -> Result<Dynamic, Box<EvalAltResult>> {
                let (db_path, config) = {
                    let ctx = ctx.lock();
                    match ctx.db_path.clone() {
                        Some(p) => (p, ctx.config.clone()),
                        None => {
                            return Err(
                                "No database opened. Call open(\"/path/to/db\") first.".into()
                            );
                        }
                    }
                };
                do_walk_wal(&db_path, &config, 0, |entry| {
                    let _: Dynamic =
                        visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                    Ok(())
                })?;
                Ok(Dynamic::UNIT)
            },
        );
    }

    // --- walk_wal(start_pos, |entry| { ... }) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "walk_wal",
            move |native_ctx: NativeCallContext,
                  start_pos: i64,
                  visitor: FnPtr|
                  -> Result<Dynamic, Box<EvalAltResult>> {
                let (db_path, config) = {
                    let ctx = ctx.lock();
                    match ctx.db_path.clone() {
                        Some(p) => (p, ctx.config.clone()),
                        None => {
                            return Err(
                                "No database opened. Call open(\"/path/to/db\") first.".into()
                            );
                        }
                    }
                };
                let start = u64::try_from(start_pos).map_err(|_| -> Box<EvalAltResult> {
                    "start_pos must be non-negative".into()
                })?;
                do_walk_wal(&db_path, &config, start, |entry| {
                    let _: Dynamic =
                        visitor.call_within_context::<Dynamic>(&native_ctx, (entry,))?;
                    Ok(())
                })?;
                Ok(Dynamic::UNIT)
            },
        );
    }

    // --- wal_stats() ---
    {
        let ctx = ctx.clone();
        engine.register_fn("wal_stats", move || -> Result<(), Box<EvalAltResult>> {
            let (db_path, config, key_shape, print) = {
                let ctx = ctx.lock();
                match ctx.db_path.clone() {
                    Some(p) => (
                        p,
                        ctx.config.clone(),
                        ctx.key_shape.clone(),
                        ctx.print_fn.clone(),
                    ),
                    None => {
                        return Err("No database opened. Call open(\"/path/to/db\") first.".into());
                    }
                }
            };

            let mut counts = [0usize; 5]; // record, remove, index, batch_start, drop_cells
            let mut total_bytes = 0usize;
            let mut ks_counts: std::collections::BTreeMap<u8, (usize, usize)> = Default::default(); // ks -> (records, removes)

            do_walk_wal(&db_path, &config, 0, |e| {
                total_bytes += e.raw_size as usize;
                match e.entry_type.as_str() {
                    "record" => {
                        counts[0] += 1;
                        ks_counts.entry(e.keyspace as u8).or_default().0 += 1;
                    }
                    "remove" => {
                        counts[1] += 1;
                        ks_counts.entry(e.keyspace as u8).or_default().1 += 1;
                    }
                    "index" => counts[2] += 1,
                    "batch_start" => counts[3] += 1,
                    "drop_cells" => counts[4] += 1,
                    _ => {}
                }
                Ok(())
            })?;

            let total = counts.iter().sum::<usize>();
            print("WAL Statistics");
            print("==============");
            print(&format!("  Total entries : {total}"));
            print(&format!("  Total bytes   : {total_bytes}"));
            print(&format!("  Records       : {}", counts[0]));
            print(&format!("  Removes       : {}", counts[1]));
            print(&format!("  Index         : {}", counts[2]));
            print(&format!("  BatchStart    : {}", counts[3]));
            print(&format!("  DropCells     : {}", counts[4]));

            if !ks_counts.is_empty() {
                print("\nPer-keyspace (records / removes):");
                for (ks_id, (recs, rems)) in &ks_counts {
                    let name = key_shape
                        .as_ref()
                        .map(|ks| {
                            let space = tidehunter::key_shape::KeySpace::new(*ks_id);
                            ks.ks(space).name().to_string()
                        })
                        .unwrap_or_else(|| format!("ks{ks_id}"));
                    print(&format!(
                        "  {name:35} records={recs:>10}  removes={rems:>10}"
                    ));
                }
            }
            Ok(())
        });
    }

    // --- help() ---
    engine.register_fn("help", || {
        println!("TideConsole — TideHunter Interactive Shell");
        println!();
        println!("Functions:");
        println!("  open(path)           Open a TideHunter database directory");
        println!("  walk_wal(visitor)           Walk WAL from the start, calling visitor(entry)");
        println!("  walk_wal(start_pos, visitor) Walk WAL from start_pos byte offset");
        println!("  wal_stats()          Print a WAL entry-type and keyspace summary");
        println!("  help()               Show this message");
        println!();
        println!("Entry fields (available inside walk_wal visitor):");
        println!("  entry.entry_type     \"record\" | \"remove\" | \"index\" | \"batch_start\" | \"drop_cells\"");
        println!("  entry.keyspace       Keyspace ID (integer)");
        println!("  entry.key            Key bytes as hex string");
        println!("  entry.value          Value bytes as hex string (records only)");
        println!("  entry.value_len      Value length in bytes");
        println!("  entry.position       Byte offset in the WAL (hex with #x prefix in Display)");
        println!("  entry.raw_size       Frame size including CRC header");
        println!();
        println!("Example:");
        println!("  open(\"/data/mydb\");");
        println!("  walk_wal(|entry| {{");
        println!("      if entry.entry_type == \"record\" && entry.keyspace == 5 {{");
        println!("          print(entry.key + \" len=\" + entry.value_len);");
        println!("      }}");
        println!("  }});");
    });

    // Redirect Rhai's built-in print/debug through ctx.print_fn so tests can capture it.
    let ctx_print = ctx.clone();
    engine.on_print(move |s| (ctx_print.lock().print_fn)(s));
    let ctx_debug = ctx.clone();
    engine.on_debug(move |s, src, pos| {
        let msg = match src {
            Some(src) => format!("[dbg {src}:{pos}] {s}"),
            None => format!("[dbg] {s}"),
        };
        (ctx_debug.lock().print_fn)(&msg);
    });

    engine
}

/// Returns true when `input` has balanced brackets/parens/braces and is safe to evaluate.
pub fn is_complete(input: &str) -> bool {
    let mut depth: i32 = 0;
    let mut in_str = false;
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if in_str {
            if ch == '\\' {
                chars.next(); // skip escaped char
            } else if ch == '"' {
                in_str = false;
            }
        } else {
            match ch {
                '"' => in_str = true,
                '{' | '(' | '[' => depth += 1,
                '}' | ')' | ']' => depth -= 1,
                _ => {}
            }
        }
    }
    depth <= 0
}

/// Initialise an engine and pre-open a database path without entering the REPL.
pub fn init_scope_with_db(
    engine: &Engine,
    scope: &mut Scope,
    _ctx: &Arc<Mutex<ConsoleContext>>,
    db_path: &str,
) -> Result<(), anyhow::Error> {
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(scope, &format!("open(\"{db_path}\")"))
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(())
}
