use super::setup_db_with_cr;
use crate::engine::{ConsoleContext, create_engine};
use parking_lot::Mutex;
use rhai::{Dynamic, Scope};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Tests that stay in Rust
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
fn test_cr_stats_runs_without_error() {
    let (_dir, path) = setup_db_with_cr();
    let output: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let output_clone = output.clone();
    let ctx = Arc::new(Mutex::new(ConsoleContext {
        print_fn: Arc::new(move |s| output_clone.lock().push(s.to_string())),
    }));
    let engine = create_engine(ctx);
    let mut scope = Scope::new();
    let path_str = path.display().to_string();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, &format!("let db = open(\"{path_str}\")"))
        .unwrap();

    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, "db.cr_stats()")
        .unwrap();
    let lines = output.lock().join("\n");
    assert!(lines.contains("Control Region Stats"), "header present");
    assert!(
        lines.contains("Index WAL Space"),
        "index space section present"
    );

    output.lock().clear();
    let _: Dynamic = engine
        .eval_with_scope::<Dynamic>(&mut scope, "db.cr_stats(1)")
        .unwrap();
    let lines2 = output.lock().join("\n");
    assert!(
        lines2.contains("Lowest 1"),
        "cr_stats(1) shows lowest 1 cell"
    );
}

#[test]
fn test_open_multiple_dbs() {
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
