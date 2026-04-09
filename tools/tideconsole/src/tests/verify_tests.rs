use super::{open_engine, setup_db};

// ---------------------------------------------------------------------------
// db.verify()
// ---------------------------------------------------------------------------

#[test]
fn test_verify_clean_db() {
    // setup_db: 5 ks writes, 2 ks deletes, 3 ks2 writes → 3 + 3 = 6 live keys
    let db = setup_db();
    let (engine, mut scope) = open_engine(&db);

    let result: rhai::Map = engine.eval_with_scope(&mut scope, "db.verify()").unwrap();

    assert_eq!(result["total"].clone().cast::<i64>(), 6, "6 live keys");
    assert_eq!(result["missing"].clone().cast::<i64>(), 0);
    assert_eq!(result["errors"].clone().cast::<i64>(), 0);
    assert_eq!(result["iter_missing"].clone().cast::<i64>(), 0);
    assert_eq!(result["iter_errors"].clone().cast::<i64>(), 0);
    assert!(result["ok"].clone().cast::<bool>(), "verify should pass");
}

#[test]
fn test_verify_verified_equals_total() {
    let db = setup_db();
    let (engine, mut scope) = open_engine(&db);

    let result: rhai::Map = engine.eval_with_scope(&mut scope, "db.verify()").unwrap();

    let total = result["total"].clone().cast::<i64>();
    let verified = result["verified"].clone().cast::<i64>();
    let iter_verified = result["iter_verified"].clone().cast::<i64>();
    assert_eq!(verified, total, "verified should equal total");
    assert_eq!(iter_verified, total, "iter_verified should equal total");
}
