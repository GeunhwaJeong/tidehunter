use super::DbHandle;
use super::wal::do_walk_wal;
use rhai::{Dynamic, Engine, EvalAltResult, Map};
use std::collections::{HashMap, HashSet};
use tidehunter::key_shape::KeySpace;

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

pub(crate) fn register(engine: &mut Engine) {
    // --- db.verify() → map ---
    //
    // Walks the WAL to collect live keys (records minus removes), opens the
    // database, then checks every live key via get() and scan().
    //
    // Returns a map:
    //   {
    //     "total":         i64,   // live keys found in WAL
    //     "verified":      i64,   // keys found via get()
    //     "missing":       i64,   // keys absent from get()
    //     "errors":        i64,   // get() returned an error
    //     "iter_verified": i64,   // keys found via scan()
    //     "iter_missing":  i64,   // keys absent from scan()
    //     "iter_errors":   i64,   // scan() encountered an error
    //     "ok":            bool,  // true iff all counts are 0 except verified/iter_verified
    //   }
    engine.register_fn(
        "verify",
        |h: &mut DbHandle| -> Result<rhai::Map, Box<EvalAltResult>> {
            let (db_path, config, print) = {
                let state = h.0.lock();
                (
                    state.db_path.clone(),
                    state.config.clone(),
                    state.print_fn.clone(),
                )
            };

            // Step 1: collect live keys from WAL (record entries minus removes)
            print("Scanning WAL for live keys...");
            let mut live: HashMap<(u8, Vec<u8>), ()> = HashMap::new();
            do_walk_wal(&db_path, &config, 0, false, |entry| {
                match entry.entry_type.as_str() {
                    "record" => {
                        live.insert((entry.keyspace as u8, entry.key), ());
                    }
                    "remove" => {
                        live.remove(&(entry.keyspace as u8, entry.key));
                    }
                    _ => {}
                }
                Ok(())
            })?;
            print(&format!("  {} live keys found", live.len()));

            // Step 2: open the database (triggers WAL replay if not already open)
            let db = h.require_db()?;

            // Step 3: verify every live key via get()
            print("Verifying via get()...");
            let mut verified = 0i64;
            let mut missing = 0i64;
            let mut errors = 0i64;
            for (ks_id, key) in live.keys() {
                match db.get(KeySpace::new(*ks_id), key) {
                    Ok(Some(_)) => verified += 1,
                    Ok(None) => missing += 1,
                    Err(_) => errors += 1,
                }
            }

            // Step 4: verify every live key via scan() (grouped by keyspace)
            print("Verifying via scan()...");
            let mut by_ks: HashMap<u8, HashSet<Vec<u8>>> = HashMap::new();
            for (ks_id, key) in live.keys() {
                by_ks.entry(*ks_id).or_default().insert(key.clone());
            }
            let mut iter_verified = 0i64;
            let mut iter_missing = 0i64;
            let mut iter_errors = 0i64;
            for (ks_id, expected) in &by_ks {
                let ks = KeySpace::new(*ks_id);
                let mut found: HashSet<Vec<u8>> = HashSet::new();
                let mut iter = db.iterator(ks);
                for item in iter.by_ref() {
                    match item {
                        Ok((key, _)) => {
                            found.insert(key.to_vec());
                        }
                        Err(_) => {
                            iter_errors += 1;
                        }
                    }
                }
                for key in expected {
                    if found.contains(key) {
                        iter_verified += 1;
                    } else {
                        iter_missing += 1;
                    }
                }
            }

            let ok = missing == 0 && errors == 0 && iter_missing == 0 && iter_errors == 0;
            print(if ok {
                "Verification PASSED"
            } else {
                "Verification FAILED"
            });

            let mut m = Map::new();
            m.insert("total".into(), Dynamic::from(live.len() as i64));
            m.insert("verified".into(), Dynamic::from(verified));
            m.insert("missing".into(), Dynamic::from(missing));
            m.insert("errors".into(), Dynamic::from(errors));
            m.insert("iter_verified".into(), Dynamic::from(iter_verified));
            m.insert("iter_missing".into(), Dynamic::from(iter_missing));
            m.insert("iter_errors".into(), Dynamic::from(iter_errors));
            m.insert("ok".into(), Dynamic::from(ok));
            Ok(m)
        },
    );
}
