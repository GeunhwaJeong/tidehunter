pub mod control_region_tests;
pub mod db_tests;
pub mod verify_tests;
pub mod wal_tests;

use crate::engine::{ConsoleContext, create_engine};
use parking_lot::Mutex;
use rhai::{Dynamic, Scope};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;
use tidehunter::config::Config;
use tidehunter::db::Db;
use tidehunter::key_shape::{KeyShapeBuilder, KeySpace, KeyType};
use tidehunter::test_utils::Metrics;

// ---------------------------------------------------------------------------
// Rhai test file runners
// ---------------------------------------------------------------------------

/// Run the `.rhai` file at `src/tests/rhai/<filename>` against a standard test DB.
/// `ks_id` and `ks2_id` are injected into the scope so scripts don't hardcode them.
pub fn run_rhai_test(db: &TestDb, filename: &str) {
    let (engine, mut scope) = open_engine(db);
    scope.push("ks_id", db.ks.as_u8() as i64);
    scope.push("ks2_id", db.ks2.as_u8() as i64);
    let path = format!("{}/src/tests/rhai/{filename}", env!("CARGO_MANIFEST_DIR"));
    let script =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {path}: {e}"));
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &script)
        .unwrap_or_else(|e| panic!("Test {filename} failed:\n{e}"));
}

/// Run the `.rhai` file at `src/tests/rhai/<filename>` against a CR test DB.
pub fn run_cr_rhai_test(db_path: &PathBuf, filename: &str) {
    let (engine, mut scope) = open_cr_engine(db_path);
    let path = format!("{}/src/tests/rhai/{filename}", env!("CARGO_MANIFEST_DIR"));
    let script =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {path}: {e}"));
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &script)
        .unwrap_or_else(|e| panic!("Test {filename} failed:\n{e}"));
}

/// Build an engine+scope for a CR test database (no pre-populated scope variables).
pub fn open_cr_engine(path: &PathBuf) -> (rhai::Engine, Scope<'static>) {
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
// Shared test helpers
// ---------------------------------------------------------------------------

pub struct TestDb {
    pub _dir: TempDir, // keeps the directory alive
    pub path: PathBuf,
    pub ks: KeySpace,
    pub ks2: KeySpace,
}

/// Create a two-keyspace database populated with known records and removes.
///
/// ks  — 5 records ("key00___".."key04___"), then 2 removes ("key00___", "key01___")
/// ks2 — 3 records ("key00___".."key02___")
pub fn setup_db() -> TestDb {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("db");
    std::fs::create_dir_all(&path).unwrap();

    let mut builder = KeyShapeBuilder::new();
    let ks = builder.add_key_space("ks", 8, 16, KeyType::uniform(8));
    let ks2 = builder.add_key_space("ks2", 8, 16, KeyType::uniform(8));
    let key_shape = builder.build();

    let config = Arc::new(Config::default());
    let metrics = Metrics::new();
    let db = Db::open(&path, key_shape, config, metrics).unwrap();

    let mut batch = db.write_batch();
    for i in 0..5u8 {
        batch.write(ks, format!("key{i:02}___").into_bytes(), vec![i; 16]);
    }
    for i in 0..3u8 {
        batch.write(ks2, format!("key{i:02}___").into_bytes(), vec![i + 10; 8]);
    }
    batch.commit().unwrap();

    let mut batch = db.write_batch();
    batch.delete(ks, b"key00___".to_vec());
    batch.delete(ks, b"key01___".to_vec());
    batch.commit().unwrap();

    drop(db);

    TestDb {
        _dir: dir,
        path,
        ks,
        ks2,
    }
}

/// Create a two-keyspace database with a forced control-region snapshot.
///
/// Keyspaces: "objects" (5 records), "metadata" (3 records).
pub fn setup_db_with_cr() -> (TempDir, PathBuf) {
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

    db.force_rebuild_control_region().unwrap();
    drop(db);

    (dir, path)
}

/// Build an engine+scope with the test DB already opened as `db` and stdout silenced.
pub fn open_engine(db: &TestDb) -> (rhai::Engine, Scope<'static>) {
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}), // silence output in tests
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let path = db.path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{path}\")"))
        .unwrap();
    (engine, scope)
}
