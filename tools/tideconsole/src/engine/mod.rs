pub(crate) mod db_fns;
pub(crate) mod wal;

use parking_lot::Mutex;
use rhai::{Dynamic, Engine, Scope};
use std::path::PathBuf;
use std::sync::Arc;
use tidehunter::config::Config;
use tidehunter::db::{Db, DbResult};
use tidehunter::key_shape::KeyShape;
use tidehunter::test_utils::WalEntry;

// ---------------------------------------------------------------------------
// Shared console state
// ---------------------------------------------------------------------------

pub struct ConsoleContext {
    pub db_path: Option<PathBuf>,
    pub config: Config,
    pub key_shape: Option<KeyShape>,
    /// Live database handle used by query/manipulation functions.
    pub db: Option<Arc<Db>>,
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
            db: None,
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
    /// Key bytes (hex-encoded lazily when accessed from Rhai)
    pub key: Vec<u8>,
    /// Value bytes (hex-encoded lazily when accessed from Rhai; empty for non-records)
    pub value: Vec<u8>,
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
            self.entry_type,
            self.keyspace,
            hex::encode(&self.key),
            self.value_len,
            self.position
        )
    }
}

pub(crate) fn from_wal_entry(entry: WalEntry, offset: u64, raw_size: usize) -> Entry {
    let position = offset as i64;
    let raw_size = raw_size as i64;
    match entry {
        WalEntry::Record(ks, key, value, _relocated) => Entry {
            entry_type: "record".into(),
            keyspace: ks.as_u8() as i64,
            value_len: value.len() as i64,
            key: key.to_vec(),
            value: value.to_vec(),
            position,
            raw_size,
        },
        WalEntry::Remove(ks, key) => Entry {
            entry_type: "remove".into(),
            keyspace: ks.as_u8() as i64,
            key: key.to_vec(),
            value: vec![],
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::Index(ks, _data) => Entry {
            entry_type: "index".into(),
            keyspace: ks.as_u8() as i64,
            key: vec![],
            value: vec![],
            value_len: 0,
            position,
            raw_size,
        },
        WalEntry::BatchStart(size) => Entry {
            entry_type: "batch_start".into(),
            keyspace: 0,
            key: vec![],
            value: vec![],
            value_len: size as i64,
            position,
            raw_size,
        },
        WalEntry::DropCells(ks, _from, _to) => Entry {
            entry_type: "drop_cells".into(),
            keyspace: ks.as_u8() as i64,
            key: vec![],
            value: vec![],
            value_len: 0,
            position,
            raw_size,
        },
    }
}

// ---------------------------------------------------------------------------
// Helpers used by db_fns
// ---------------------------------------------------------------------------

/// Decode a hex string to bytes, returning an EvalAltResult on failure.
pub(crate) fn decode_hex(s: &str) -> Result<Vec<u8>, Box<rhai::EvalAltResult>> {
    hex::decode(s).map_err(|e| -> Box<rhai::EvalAltResult> {
        format!("Invalid hex string \"{s}\": {e}").into()
    })
}

/// Map a DbResult to an EvalAltResult.
pub(crate) fn db_err<T>(r: DbResult<T>) -> Result<T, Box<rhai::EvalAltResult>> {
    r.map_err(|e| -> Box<rhai::EvalAltResult> { format!("Database error: {e:?}").into() })
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
    engine.register_get("key", |e: &mut Entry| hex::encode(&e.key));
    engine.register_get("value", |e: &mut Entry| hex::encode(&e.value));
    engine.register_get("value_len", |e: &mut Entry| e.value_len);
    engine.register_get("position", |e: &mut Entry| e.position);
    engine.register_get("raw_size", |e: &mut Entry| e.raw_size);
    engine.register_fn("to_string", |e: &mut Entry| e.to_string());
    engine.register_fn("to_debug", |e: &mut Entry| format!("{e:?}"));

    db_fns::register(&mut engine, ctx.clone());
    wal::register(&mut engine, ctx.clone());

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
