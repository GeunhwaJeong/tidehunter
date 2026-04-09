use crate::engine::{ConsoleContext, create_engine};
use parking_lot::Mutex;
use rhai::{Dynamic, Scope};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShapeBuilder, KeyType};
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn setup_db_with_cr() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    let ks = builder.add_key_space("objects", 8, 16, KeyType::uniform(8));
    let ks2 = builder.add_key_space("metadata", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let config = Arc::new(Config::default());
    let db = Db::open(&path, key_shape, config, Metrics::new()).unwrap();

    let mut batch = db.write_batch();
    for i in 0..5u8 {
        batch.write(ks, format!("key{i:02}___").into_bytes(), vec![i; 16]);
    }
    for i in 0..3u8 {
        batch.write(ks2, format!("key{i:02}___").into_bytes(), vec![i + 10; 8]);
    }
    batch.commit().unwrap();

    // Force CR write so the file exists on disk.
    db.force_rebuild_control_region().unwrap();
    drop(db);

    (dir, path)
}

fn open_cr_engine(path: &PathBuf) -> (rhai::Engine, Scope<'static>) {
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let path_str = path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{path_str}\")"))
        .unwrap();
    (engine, scope)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_open_invalid_path_returns_error() {
    // Opening a nonexistent path should fail at key_shape loading.
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let result = engine.eval_with_scope::<Dynamic>(&mut scope, "open(\"/nonexistent/path/to/db\")");
    assert!(result.is_err(), "open() on a nonexistent path should fail");
    assert!(
        result.unwrap_err().to_string().contains("key shape"),
        "Error should mention 'key shape'"
    );
}

#[test]
fn test_load_cr_structure() {
    let (_dir, path) = setup_db_with_cr();
    let (engine, mut scope) = open_cr_engine(&path);

    let cr: rhai::Map = engine.eval_with_scope(&mut scope, "db.load_cr()").unwrap();

    // last_position is present
    assert!(cr["last_position"].clone().cast::<i64>() >= 0);

    // two keyspaces
    let keyspaces = cr["keyspaces"].clone().cast::<rhai::Array>();
    assert_eq!(keyspaces.len(), 2);

    let ks0 = keyspaces[0].clone().cast::<rhai::Map>();
    assert_eq!(ks0["name"].clone().cast::<String>(), "objects");
    assert_eq!(ks0["id"].clone().cast::<i64>(), 0);

    // Uniform KS pre-populates all cells, so cells array is non-empty
    let cells = ks0["cells"].clone().cast::<rhai::Array>();
    assert!(!cells.is_empty());

    // Each cell has the expected raw fields
    let cell = cells[0].clone().cast::<rhai::Map>();
    let offset = cell["offset"].clone().cast::<i64>();
    let len = cell["len"].clone().cast::<i64>();
    let lp = cell["last_processed"].clone().cast::<i64>();
    assert!(
        offset == -1 || offset >= 0,
        "offset must be -1 (invalid) or ≥0"
    );
    assert!(len >= 0);
    assert!(lp >= 0);
}

#[test]
fn test_load_cr_valid_cells_after_snapshot() {
    let (_dir, path) = setup_db_with_cr();
    let (engine, mut scope) = open_cr_engine(&path);

    // After force_rebuild_control_region, at least some cells should have valid offsets
    let result: Dynamic = engine
        .eval_with_scope(
            &mut scope,
            r#"
            let cr = db.load_cr();
            let valid = 0;
            for ks in cr.keyspaces {
                for c in ks.cells {
                    if c.offset >= 0 { valid += 1; }
                }
            }
            valid
            "#,
        )
        .unwrap();
    assert!(
        result.cast::<i64>() > 0,
        "Expected some valid cells after snapshot"
    );
}

#[test]
fn test_force_snapshot_returns_position() {
    let (_dir, path) = setup_db_with_cr();
    let (engine, mut scope) = open_cr_engine(&path);

    let pos: i64 = engine
        .eval_with_scope(&mut scope, "db.force_snapshot()")
        .unwrap();
    assert!(
        pos >= 0,
        "force_snapshot should return a non-negative WAL position, got {pos}"
    );

    // CR is still readable after the snapshot
    let cr: rhai::Map = engine.eval_with_scope(&mut scope, "db.load_cr()").unwrap();
    assert!(cr["last_position"].clone().cast::<i64>() >= 0);
}

#[test]
fn test_open_multiple_dbs() {
    // Verify that two db handles can be opened and queried independently.
    let (dir1, path1) = setup_db_with_cr();
    let (dir2, path2) = setup_db_with_cr();

    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();

    let p1 = path1.display().to_string();
    let p2 = path2.display().to_string();
    let script = format!(
        r#"
        let db1 = open("{p1}");
        let db2 = open("{p2}");
        let cr1 = db1.load_cr();
        let cr2 = db2.load_cr();
        [cr1.keyspaces.len(), cr2.keyspaces.len()]
        "#
    );
    let result: rhai::Array = engine.eval_with_scope(&mut scope, &script).unwrap();
    assert_eq!(
        result[0].clone().cast::<i64>(),
        2,
        "db1 should have 2 keyspaces"
    );
    assert_eq!(
        result[1].clone().cast::<i64>(),
        2,
        "db2 should have 2 keyspaces"
    );

    drop((dir1, dir2));
}
