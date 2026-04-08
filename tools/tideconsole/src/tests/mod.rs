pub mod control_region_tests;
pub mod db_tests;
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

/// Build an engine+scope with the test DB already opened and stdout silenced.
pub fn open_engine(db: &TestDb) -> (rhai::Engine, Scope<'static>) {
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(|_| {}), // silence output in tests
        ..ConsoleContext::default()
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let path = db.path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("open(\"{path}\")"))
        .unwrap();
    (engine, scope)
}
