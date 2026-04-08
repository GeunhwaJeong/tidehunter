use super::{ConsoleContext, db_err, decode_hex};
use parking_lot::Mutex;
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, NativeCallContext};
use std::path::PathBuf;
use std::sync::Arc;
use tidehunter::db::Db;
use tidehunter::key_shape::KeySpace;
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine, ctx: Arc<Mutex<ConsoleContext>>) {
    // --- open(path) ---
    // Loads config and key shape only; WAL replay is deferred until the first CRUD operation.
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
                ctx.db = None; // drop any previously open handle
                (ctx.print_fn)(&format!(
                    "Loaded {path} (WAL replay deferred until first query)"
                ));
                Ok(())
            },
        );
    }

    // --- get(ks, key_hex) → string | () ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "get",
            move |ks: Dynamic, key_hex: &str| -> Result<Dynamic, Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let key = decode_hex(key_hex)?;
                let result = db_err(db.get(ks, &key))?;
                Ok(match result {
                    Some(v) => Dynamic::from(hex::encode(v.as_ref())),
                    None => Dynamic::UNIT,
                })
            },
        );
    }

    // --- exists(ks, key_hex) → bool ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "exists",
            move |ks: Dynamic, key_hex: &str| -> Result<bool, Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let key = decode_hex(key_hex)?;
                db_err(db.exists(ks, &key))
            },
        );
    }

    // --- put(ks, key_hex, value_hex) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "put",
            move |ks: Dynamic, key_hex: &str, value_hex: &str| -> Result<(), Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let key = decode_hex(key_hex)?;
                let value = decode_hex(value_hex)?;
                db_err(db.insert(ks, key, value))
            },
        );
    }

    // --- delete(ks, key_hex) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "delete",
            move |ks: Dynamic, key_hex: &str| -> Result<(), Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let key = decode_hex(key_hex)?;
                db_err(db.remove(ks, key))
            },
        );
    }

    // --- scan(ks, visitor) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "scan",
            move |native_ctx: NativeCallContext,
                  ks: Dynamic,
                  visitor: FnPtr|
                  -> Result<Dynamic, Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let mut iter = db.iterator(ks);
                run_scan_iter(&mut iter, &native_ctx, &visitor)?;
                Ok(Dynamic::UNIT)
            },
        );
    }

    // --- scan(ks, lower_bound_hex, visitor) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "scan",
            move |native_ctx: NativeCallContext,
                  ks: Dynamic,
                  lower_hex: &str,
                  visitor: FnPtr|
                  -> Result<Dynamic, Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let lower = decode_hex(lower_hex)?;
                let mut iter = db.iterator(ks);
                iter.set_lower_bound(lower);
                run_scan_iter(&mut iter, &native_ctx, &visitor)?;
                Ok(Dynamic::UNIT)
            },
        );
    }

    // --- scan(ks, lower_bound_hex, upper_bound_hex, visitor) ---
    {
        let ctx = ctx.clone();
        engine.register_fn(
            "scan",
            move |native_ctx: NativeCallContext,
                  ks: Dynamic,
                  lower_hex: &str,
                  upper_hex: &str,
                  visitor: FnPtr|
                  -> Result<Dynamic, Box<EvalAltResult>> {
                let (db, ks) = require_db_and_ks(&ctx, ks)?;
                let lower = decode_hex(lower_hex)?;
                let upper = decode_hex(upper_hex)?;
                let mut iter = db.iterator(ks);
                iter.set_lower_bound(lower);
                iter.set_upper_bound(upper);
                run_scan_iter(&mut iter, &native_ctx, &visitor)?;
                Ok(Dynamic::UNIT)
            },
        );
    }

    // --- help() ---
    engine.register_fn("help", || {
        println!("TideConsole — TideHunter Interactive Shell");
        println!();
        println!("Functions:");
        println!("  open(path)                           Open a TideHunter database directory");
        println!("  get(ks, key_hex)                     Look up a key; returns value hex or ()");
        println!("  exists(ks, key_hex)                  Check if a key exists");
        println!("  put(ks, key_hex, value_hex)          Write a key-value record");
        println!("  delete(ks, key_hex)                  Delete a key");
        println!("  scan(ks, visitor)                    Iterate all live keys in a keyspace");
        println!("  scan(ks, lower, visitor)             Iterate from lower bound (inclusive)");
        println!("  scan(ks, lower, upper, visitor)      Iterate between bounds");
        println!("  walk_wal(visitor)                    Walk WAL from the start");
        println!("  walk_wal(start_pos, visitor)         Walk WAL from start_pos byte offset");
        println!(
            "  list_wal_files()                     List WAL files with start_pos, size, created"
        );
        println!(
            "  wal_stats()                          Print a WAL entry-type and keyspace summary"
        );
        println!("  help()                               Show this message");
        println!();
        println!("ks accepts an integer keyspace ID (e.g. 0) or a name string (e.g. \"objects\").");
        println!("scan visitor receives (key_hex, value_hex) as separate string arguments.");
        println!("walk_wal visitor receives an Entry object — use entry.key, entry.value, etc.");
        println!();
        println!("Example:");
        println!("  open(\"/data/mydb\");");
        println!("  scan(\"objects\", |key, value| {{");
        println!("      print(key + \" -> \" + value.len() + \" bytes\");");
        println!("  }});");
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve a Rhai `Dynamic` value to a `KeySpace`.
/// Accepts an integer keyspace ID (`i64`) or a keyspace name (`String`).
fn resolve_ks(ctx: &ConsoleContext, ks: Dynamic) -> Result<KeySpace, Box<EvalAltResult>> {
    if ks.is::<i64>() {
        return Ok(KeySpace::new(ks.cast::<i64>() as u8));
    }
    if ks.is::<String>() {
        let name = ks.cast::<String>();
        let key_shape = ctx
            .key_shape
            .as_ref()
            .ok_or_else(|| -> Box<EvalAltResult> {
                "No database opened. Call open(\"/path/to/db\") first.".into()
            })?;
        return key_shape
            .iter_ks()
            .find(|ksd| ksd.name() == name)
            .map(|ksd| ksd.id())
            .ok_or_else(|| -> Box<EvalAltResult> {
                format!("Unknown keyspace \"{name}\"").into()
            });
    }
    Err("ks must be an integer ID or a name string".into())
}

fn require_db_and_ks(
    ctx: &Arc<Mutex<ConsoleContext>>,
    ks: Dynamic,
) -> Result<(Arc<Db>, KeySpace), Box<EvalAltResult>> {
    let mut ctx_guard = ctx.lock();
    if ctx_guard.db.is_none() {
        let db_path = ctx_guard
            .db_path
            .as_ref()
            .ok_or_else(|| -> Box<EvalAltResult> {
                "No database opened. Call open(\"/path/to/db\") first.".into()
            })?
            .clone();
        (ctx_guard.print_fn)("Opening database (replaying WAL, this may take a while)...");
        let key_shape = ctx_guard.key_shape.clone().unwrap();
        let config = ctx_guard.config.clone();
        let db = Db::open(&db_path, key_shape, Arc::new(config), Metrics::new()).map_err(
            |e| -> Box<EvalAltResult> { format!("Failed to open database: {e:?}").into() },
        )?;
        ctx_guard.db = Some(db);
    }
    let db = ctx_guard.db.clone().unwrap();
    let ks = resolve_ks(&ctx_guard, ks)?;
    Ok((db, ks))
}

fn run_scan_iter(
    iter: &mut tidehunter::iterators::db_iterator::DbIterator,
    native_ctx: &NativeCallContext,
    visitor: &FnPtr,
) -> Result<(), Box<EvalAltResult>> {
    for item in iter.by_ref() {
        let (key, value) =
            item.map_err(|e| -> Box<EvalAltResult> { format!("Iterator error: {e:?}").into() })?;
        let key_hex = Dynamic::from(hex::encode(key.as_ref()));
        let value_hex = Dynamic::from(hex::encode(value.as_ref()));
        let _: Dynamic =
            visitor.call_within_context::<Dynamic>(native_ctx, (key_hex, value_hex))?;
    }
    Ok(())
}
