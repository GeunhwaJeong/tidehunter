use super::{DbHandle, db_err, decode_hex};
use rhai::{Dynamic, Engine, EvalAltResult, FnPtr, NativeCallContext};

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine) {
    // --- db.get(ks, key_hex) → string | () ---
    engine.register_fn(
        "get",
        |h: &mut DbHandle, ks: Dynamic, key_hex: &str| -> Result<Dynamic, Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let key = decode_hex(key_hex)?;
            let result = db_err(db.get(ks, &key))?;
            Ok(match result {
                Some(v) => Dynamic::from(hex::encode(v.as_ref())),
                None => Dynamic::UNIT,
            })
        },
    );

    // --- db.exists(ks, key_hex) → bool ---
    engine.register_fn(
        "exists",
        |h: &mut DbHandle, ks: Dynamic, key_hex: &str| -> Result<bool, Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let key = decode_hex(key_hex)?;
            db_err(db.exists(ks, &key))
        },
    );

    // --- db.put(ks, key_hex, value_hex) ---
    engine.register_fn(
        "put",
        |h: &mut DbHandle,
         ks: Dynamic,
         key_hex: &str,
         value_hex: &str|
         -> Result<(), Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let key = decode_hex(key_hex)?;
            let value = decode_hex(value_hex)?;
            db_err(db.insert(ks, key, value))
        },
    );

    // --- db.delete(ks, key_hex) ---
    engine.register_fn(
        "delete",
        |h: &mut DbHandle, ks: Dynamic, key_hex: &str| -> Result<(), Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let key = decode_hex(key_hex)?;
            db_err(db.remove(ks, key))
        },
    );

    // --- db.scan(ks, visitor) ---
    engine.register_fn(
        "scan",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         ks: Dynamic,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let mut iter = db.iterator(ks);
            run_scan_iter(&mut iter, &native_ctx, &visitor)?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.scan(ks, lower_bound_hex, visitor) ---
    engine.register_fn(
        "scan",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         ks: Dynamic,
         lower_hex: &str,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let lower = decode_hex(lower_hex)?;
            let mut iter = db.iterator(ks);
            iter.set_lower_bound(lower);
            run_scan_iter(&mut iter, &native_ctx, &visitor)?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- db.scan(ks, lower_bound_hex, upper_bound_hex, visitor) ---
    engine.register_fn(
        "scan",
        |native_ctx: NativeCallContext,
         h: &mut DbHandle,
         ks: Dynamic,
         lower_hex: &str,
         upper_hex: &str,
         visitor: FnPtr|
         -> Result<Dynamic, Box<EvalAltResult>> {
            let (db, ks) = h.require_db_and_ks(ks)?;
            let lower = decode_hex(lower_hex)?;
            let upper = decode_hex(upper_hex)?;
            let mut iter = db.iterator(ks);
            iter.set_lower_bound(lower);
            iter.set_upper_bound(upper);
            run_scan_iter(&mut iter, &native_ctx, &visitor)?;
            Ok(Dynamic::UNIT)
        },
    );

    // --- help() ---
    engine.register_fn("help", || {
        println!("TideConsole — TideHunter Interactive Shell");
        println!();
        println!("Opening databases:");
        println!("  let db = open(path)                  Open a database; returns a db handle");
        println!("  (when --db is used, the handle is available as 'db' automatically)");
        println!();
        println!("Database methods (call as db.method(...)):");
        println!("  db.get(ks, key_hex)                  Look up a key; returns value hex or ()");
        println!("  db.exists(ks, key_hex)               Check if a key exists");
        println!("  db.put(ks, key_hex, value_hex)       Write a key-value record");
        println!("  db.delete(ks, key_hex)               Delete a key");
        println!("  db.scan(ks, visitor)                 Iterate all live keys in a keyspace");
        println!("  db.scan(ks, lower, visitor)          Iterate from lower bound (inclusive)");
        println!("  db.scan(ks, lower, upper, visitor)   Iterate between bounds");
        println!("  db.walk_wal(visitor)                 Walk WAL from the start");
        println!("  db.walk_wal(start_pos, visitor)      Walk WAL from start_pos byte offset");
        println!("  db.list_wal_files()                  List WAL files with start_pos, size, created");
        println!("  db.wal_stats()                       Print a WAL entry-type and keyspace summary");
        println!("  db.load_cr()                         Load control region into a Rhai map (no WAL replay)");
        println!("  db.load_index(offset)                Inspect on-disk index at given offset");
        println!();
        println!("  help()                               Show this message");
        println!();
        println!("ks accepts an integer keyspace ID (e.g. 0) or a name string (e.g. \"objects\").");
        println!("scan visitor receives (key_hex, value_hex) as separate string arguments.");
        println!("walk_wal visitor receives an Entry object — use entry.key, entry.value, etc.");
        println!();
        println!("Example:");
        println!("  let db = open(\"/data/mydb\");");
        println!("  db.scan(\"objects\", |key, value| {{");
        println!("      print(key + \" -> \" + value.len() + \" bytes\");");
        println!("  }});");
        println!();
        println!("Multiple databases:");
        println!("  let db1 = open(\"/data/db1\");");
        println!("  let db2 = open(\"/data/db2\");");
        println!("  db1.wal_stats();");
        println!("  db2.wal_stats();");
    });
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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
