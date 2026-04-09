use super::{open_engine, setup_db};
use crate::engine::{ConsoleContext, create_engine, is_complete};
use parking_lot::Mutex;
use rhai::{Array, Dynamic, Scope};
use std::sync::Arc;
use tempfile::TempDir;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShapeBuilder, KeyType};
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// is_complete (pure, no DB needed)
// ---------------------------------------------------------------------------

#[test]
fn test_is_complete_single_line() {
    assert!(is_complete("1 + 2"));
    assert!(is_complete("db.walk_wal(|e| { print(e.key); });"));
}

#[test]
fn test_is_complete_incomplete() {
    assert!(!is_complete("db.walk_wal(|entry| {"));
    assert!(!is_complete("let x = (1 +"));
}

#[test]
fn test_is_complete_multiline_complete() {
    let input = "db.walk_wal(|entry| {\n    print(entry.key);\n});";
    assert!(is_complete(input));
}

// ---------------------------------------------------------------------------
// walk_wal — entry counts
// ---------------------------------------------------------------------------

#[test]
fn test_walk_wal_total_entry_counts() {
    let db = setup_db();
    let (engine, mut scope) = open_engine(&db);

    let result: Array = engine
        .eval_with_scope(
            &mut scope,
            r#"
            let types = [];
            db.walk_wal(|entry| { types.push(entry.entry_type); });
            types
            "#,
        )
        .unwrap();

    let type_strings: Vec<String> = result.into_iter().map(|d| d.cast::<String>()).collect();
    let records = type_strings
        .iter()
        .filter(|s| s.as_str() == "record")
        .count();
    let removes = type_strings
        .iter()
        .filter(|s| s.as_str() == "remove")
        .count();

    assert_eq!(records, 8, "5 ks records + 3 ks2 records");
    assert_eq!(removes, 2, "2 removes on ks");
}

// ---------------------------------------------------------------------------
// walk_wal — filtering by keyspace
// ---------------------------------------------------------------------------

#[test]
fn test_walk_wal_filter_by_keyspace() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let ks2_id = db.ks2.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let snippet = format!(
        r#"
        let ks_records = 0;
        let ks2_records = 0;
        db.walk_wal(|entry| {{
            if entry.entry_type == "record" {{
                if entry.keyspace == {ks_id} {{ ks_records += 1; }}
                if entry.keyspace == {ks2_id} {{ ks2_records += 1; }}
            }}
        }});
        [ks_records, ks2_records]
        "#
    );

    let result: Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    let counts: Vec<i64> = result.into_iter().map(|d| d.cast::<i64>()).collect();

    assert_eq!(counts[0], 5, "5 records in ks");
    assert_eq!(counts[1], 3, "3 records in ks2");
}

// ---------------------------------------------------------------------------
// walk_wal — entry field values for a known record
// ---------------------------------------------------------------------------

#[test]
fn test_walk_wal_record_fields() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let snippet = format!(
        r#"
        let found = #{{}};
        db.walk_wal(|entry| {{
            if entry.entry_type == "record" && entry.keyspace == {ks_id} && found.is_empty() {{
                found = #{{
                    "key_len": entry.key.len(),
                    "value_len": entry.value_len,
                    "value_hex_len": entry.value.len(),
                    "raw_size_positive": entry.raw_size > 0,
                    "position_non_neg": entry.position >= 0,
                }};
            }}
        }});
        found
        "#
    );

    let result = engine
        .eval_with_scope::<Dynamic>(&mut scope, &snippet)
        .unwrap();
    let map = result.cast::<rhai::Map>();

    // key "key00___" = 8 bytes → 16 hex chars
    assert_eq!(map["key_len"].clone().cast::<i64>(), 16);
    // value is vec![0u8; 16] → 16 bytes → 32 hex chars
    assert_eq!(map["value_len"].clone().cast::<i64>(), 16);
    assert_eq!(map["value_hex_len"].clone().cast::<i64>(), 32);
    assert!(map["raw_size_positive"].clone().cast::<bool>());
    assert!(map["position_non_neg"].clone().cast::<bool>());
}

// ---------------------------------------------------------------------------
// walk_wal — remove entry key round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_walk_wal_remove_keys() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let snippet = format!(
        r#"
        let remove_keys = [];
        db.walk_wal(|entry| {{
            if entry.entry_type == "remove" && entry.keyspace == {ks_id} {{
                remove_keys.push(entry.key);
            }}
        }});
        remove_keys
        "#
    );

    let result: Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    let keys: Vec<String> = result.into_iter().map(|d| d.cast::<String>()).collect();

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&hex::encode(b"key00___")));
    assert!(keys.contains(&hex::encode(b"key01___")));
}

// ---------------------------------------------------------------------------
// walk_wal(start_pos, visitor) — resumes from a given WAL offset
// ---------------------------------------------------------------------------

#[test]
fn test_walk_wal_from_position() {
    let db = setup_db();
    let (engine, mut scope) = open_engine(&db);

    // First pass: collect all entry positions to get the offset after the first entry.
    let all_positions: Array = engine
        .eval_with_scope(
            &mut scope,
            r#"
            let positions = [];
            db.walk_wal(|entry| { positions.push(entry.position); });
            positions
            "#,
        )
        .unwrap();
    assert!(
        all_positions.len() >= 2,
        "need at least 2 entries to test seeking"
    );
    let total = all_positions.len();

    // The second entry's position is where we want to resume from.
    let resume_pos = all_positions[1].clone().cast::<i64>();

    // Second pass: walk from that position and count entries seen.
    let snippet = format!(
        r#"
        let count = 0;
        db.walk_wal({resume_pos}, |entry| {{ count += 1; }});
        count
        "#
    );
    let count_from_pos = engine.eval_with_scope::<i64>(&mut scope, &snippet).unwrap();

    assert_eq!(
        count_from_pos,
        (total - 1) as i64,
        "walking from the second entry's position should skip exactly one entry"
    );
}

// ---------------------------------------------------------------------------
// list_wal_files — multi-file WAL
// ---------------------------------------------------------------------------

#[test]
fn test_list_wal_files() {
    // Build a DB with a tiny wal_file_size so writes spill across multiple files.
    let wal_file_size: u64 = 1024;

    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    let ks = builder.add_key_space("ks", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let config = Arc::new(Config {
        frag_size: wal_file_size,
        wal_file_size,
        ..Config::default()
    });
    let metrics = Metrics::new();
    let db = Db::open(&path, key_shape, config, metrics).unwrap();

    // Write enough records to span several WAL files.
    for i in 0..100u8 {
        let mut batch = db.write_batch();
        batch.write(ks, format!("key{i:02}__x").into_bytes(), vec![i; 8]);
        batch.commit().unwrap();
    }
    drop(db);

    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let db_path = path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{db_path}\")"))
        .unwrap();

    let files: Array = engine
        .eval_with_scope(&mut scope, "db.list_wal_files()")
        .unwrap();

    // Must have produced multiple WAL files.
    assert!(
        files.len() >= 2,
        "expected multiple WAL files, got {}",
        files.len()
    );

    for (i, entry) in files.iter().enumerate() {
        let map = entry.clone().cast::<rhai::Map>();

        // name field is present and looks like a WAL file
        let name = map["name"].clone().cast::<String>();
        assert!(name.starts_with("wal_"), "unexpected file name: {name}");

        // start_pos for file i = i * wal_file_size
        let expected_start = (i as i64) * (wal_file_size as i64);
        assert_eq!(
            map["start_pos"].clone().cast::<i64>(),
            expected_start,
            "file {i} start_pos mismatch"
        );

        // size is positive
        assert!(
            map["size"].clone().cast::<i64>() > 0,
            "file {i} size should be positive"
        );

        // created timestamp is a plausible Unix epoch second (after year 2000)
        let created = map["created"].clone().cast::<i64>();
        assert!(
            created > 946_684_800,
            "file {i} created timestamp looks wrong: {created}"
        );
    }

    // Verify start_pos can be fed into db.walk_wal: walking from the second file's
    // start_pos must produce fewer entries than walking from position 0.
    let second_start = files[1].clone().cast::<rhai::Map>()["start_pos"]
        .clone()
        .cast::<i64>();
    let snippet = format!(
        r#"
        let full = 0;
        let partial = 0;
        db.walk_wal(|entry| {{ full += 1; }});
        db.walk_wal({second_start}, |entry| {{ partial += 1; }});
        [full, partial]
        "#
    );
    let counts: Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    let full = counts[0].clone().cast::<i64>();
    let partial = counts[1].clone().cast::<i64>();
    assert!(
        partial < full,
        "walking from second file should yield fewer entries"
    );
    assert!(
        partial > 0,
        "walking from second file should still yield entries"
    );
}

// ---------------------------------------------------------------------------
// wal_stats — output contains expected counts
// ---------------------------------------------------------------------------

#[test]
fn test_wal_stats_output() {
    let db = setup_db();
    let output: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let output_clone = output.clone();
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(move |s| output_clone.lock().push(s.to_string())),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();

    let path = db.path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{path}\")"))
        .unwrap();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, "db.wal_stats()")
        .unwrap();

    let lines = output.lock().join("\n");
    assert!(lines.contains("Records"), "output should mention Records");
    assert!(lines.contains("Removes"), "output should mention Removes");
    // 8 records total, 2 removes
    assert!(lines.contains('8'), "output should contain record count 8");
    assert!(lines.contains('2'), "output should contain remove count 2");
}

// ---------------------------------------------------------------------------
// load_index — reads the on-disk index from the index WAL
// ---------------------------------------------------------------------------

#[test]
fn test_load_index_returns_entries() {
    // Build a DB, force a snapshot so there is an on-disk index, then verify
    // that db.load_index(offset) returns the expected key entries.
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    let ks = builder.add_key_space("objects", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();
    let config = Arc::new(Config::default());
    let db = Db::open(&path, key_shape, config, Metrics::new()).unwrap();

    // Write a few records so the index is non-empty after a snapshot.
    let mut batch = db.write_batch();
    for i in 0..3u8 {
        batch.write(ks, format!("key{i:02}___").into_bytes(), vec![i; 16]);
    }
    batch.commit().unwrap();
    db.force_rebuild_control_region().unwrap();
    drop(db);

    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let db_path = path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{db_path}\")"))
        .unwrap();

    // Use db.load_cr() to find a cell with a valid index offset, then call db.load_index().
    let script = r#"
        let cr = db.load_cr();
        let idx_offset = -1;
        for ks in cr.keyspaces {
            if ks.name == "objects" {
                for c in ks.cells {
                    if c.offset >= 0 {
                        idx_offset = c.offset;
                        break;
                    }
                }
            }
        }
        idx_offset
    "#;
    let idx_offset: i64 = engine.eval_with_scope(&mut scope, script).unwrap();
    assert!(
        idx_offset >= 0,
        "expected at least one cell with a valid index offset after snapshot"
    );

    // db.load_index should return an array of maps with "key" and "wal_position" fields.
    let entries: Array = engine
        .eval_with_scope(&mut scope, &format!("db.load_index({idx_offset})"))
        .unwrap();

    // The snapshot was forced, so all 3 keys should appear across the cells.
    // Here we just check the structure of one entry.
    assert!(
        !entries.is_empty(),
        "expected at least one entry in the index at offset {idx_offset}"
    );
    let first = entries[0].clone().cast::<rhai::Map>();
    assert!(
        first.contains_key("key"),
        "each entry should have a 'key' field"
    );
    assert!(
        first.contains_key("wal_position"),
        "each entry should have a 'wal_position' field"
    );
    let key_hex = first["key"].clone().cast::<String>();
    assert!(!key_hex.is_empty(), "key hex string should be non-empty");
    let wal_pos: i64 = first["wal_position"].clone().cast::<i64>();
    assert!(wal_pos >= 0, "wal_position should be non-negative");
}

#[test]
fn test_load_index_invalid_offset() {
    let db = setup_db();
    let (engine, mut scope) = open_engine(&db);

    // -1 means "no index" — should return an error.
    let result = engine.eval_with_scope::<Array>(&mut scope, "db.load_index(-1)");
    assert!(result.is_err(), "db.load_index(-1) should fail");
    assert!(
        result.unwrap_err().to_string().contains("negative"),
        "error should mention negative offset"
    );
}
