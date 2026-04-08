use super::{open_engine, setup_db};
use rhai::{Array, Dynamic};

// ---------------------------------------------------------------------------
// get — point lookup
// ---------------------------------------------------------------------------

#[test]
fn test_get_existing_key() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    // "key02___" was written with value vec![2u8; 16]
    let key_hex = hex::encode(b"key02___");
    let expected_value_hex = hex::encode(vec![2u8; 16]);

    let snippet = format!("get({ks_id}, \"{key_hex}\")");
    let result: Dynamic = engine.eval_with_scope(&mut scope, &snippet).unwrap();

    assert!(!result.is_unit(), "key should be found");
    assert_eq!(result.cast::<String>(), expected_value_hex);
}

#[test]
fn test_get_deleted_key_returns_unit() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    // "key00___" was deleted
    let key_hex = hex::encode(b"key00___");
    let snippet = format!("get({ks_id}, \"{key_hex}\")");
    let result: Dynamic = engine.eval_with_scope(&mut scope, &snippet).unwrap();

    assert!(result.is_unit(), "deleted key should return ()");
}

#[test]
fn test_get_missing_key_returns_unit() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let key_hex = hex::encode(b"nokey___");
    let snippet = format!("get({ks_id}, \"{key_hex}\")");
    let result: Dynamic = engine.eval_with_scope(&mut scope, &snippet).unwrap();

    assert!(result.is_unit(), "missing key should return ()");
}

// ---------------------------------------------------------------------------
// exists
// ---------------------------------------------------------------------------

#[test]
fn test_exists_present_key() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let key_hex = hex::encode(b"key03___");
    let snippet = format!("exists({ks_id}, \"{key_hex}\")");
    let found: bool = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    assert!(found);
}

#[test]
fn test_exists_deleted_key() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let key_hex = hex::encode(b"key00___");
    let snippet = format!("exists({ks_id}, \"{key_hex}\")");
    let found: bool = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    assert!(!found, "deleted key should not exist");
}

#[test]
fn test_exists_missing_key() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let key_hex = hex::encode(b"nokey___");
    let snippet = format!("exists({ks_id}, \"{key_hex}\")");
    let found: bool = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    assert!(!found);
}

// ---------------------------------------------------------------------------
// put + get round-trip
// ---------------------------------------------------------------------------

#[test]
fn test_put_and_get() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let key_hex = hex::encode(b"newkey__");
    let value_hex = hex::encode(b"helloworld______"); // 16 bytes

    let snippet = format!(
        r#"
        put({ks_id}, "{key_hex}", "{value_hex}");
        get({ks_id}, "{key_hex}")
        "#
    );
    let result: Dynamic = engine.eval_with_scope(&mut scope, &snippet).unwrap();

    assert!(!result.is_unit());
    assert_eq!(result.cast::<String>(), value_hex);
}

// ---------------------------------------------------------------------------
// delete
// ---------------------------------------------------------------------------

#[test]
fn test_delete_existing_key() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    let key_hex = hex::encode(b"key04___");

    let snippet = format!(
        r#"
        let before = exists({ks_id}, "{key_hex}");
        delete({ks_id}, "{key_hex}");
        let after = exists({ks_id}, "{key_hex}");
        [before, after]
        "#
    );
    let result: Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    assert!(
        result[0].clone().cast::<bool>(),
        "key should exist before delete"
    );
    assert!(
        !result[1].clone().cast::<bool>(),
        "key should be gone after delete"
    );
}

// ---------------------------------------------------------------------------
// scan — full keyspace
// ---------------------------------------------------------------------------

#[test]
fn test_scan_counts_live_records() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    // ks has 5 records written, 2 removed → 3 live
    let snippet = format!(
        r#"
        let count = 0;
        scan({ks_id}, |key, value| {{ count += 1; }});
        count
        "#
    );
    let count: i64 = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    assert_eq!(count, 3, "3 live records in ks after 2 deletes");
}

#[test]
fn test_scan_returns_hex_key_and_value() {
    let db = setup_db();
    let ks2_id = db.ks2.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    // ks2 has 3 records with 8-byte values encoded as hex (16 hex chars each)
    let snippet = format!(
        r#"
        let value_hex_lens = [];
        scan({ks2_id}, |key, value| {{
            value_hex_lens.push(value.len());
        }});
        value_hex_lens
        "#
    );
    let lens: rhai::Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    assert_eq!(lens.len(), 3);
    for len in lens {
        assert_eq!(len.cast::<i64>(), 16, "8 bytes → 16 hex chars");
    }
}

// ---------------------------------------------------------------------------
// scan with bounds
// ---------------------------------------------------------------------------

#[test]
fn test_scan_with_lower_bound() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    // Live keys in ks: key02___, key03___, key04___
    // lower bound = key03___ → should return key03___ and key04___
    let lower_hex = hex::encode(b"key03___");

    let snippet = format!(
        r#"
        let keys = [];
        scan({ks_id}, "{lower_hex}", |key, value| {{ keys.push(key); }});
        keys
        "#
    );
    let keys: rhai::Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    let key_strings: Vec<String> = keys.into_iter().map(|d| d.cast::<String>()).collect();

    assert!(
        key_strings.contains(&hex::encode(b"key03___")),
        "key03 should be included"
    );
    assert!(
        key_strings.contains(&hex::encode(b"key04___")),
        "key04 should be included"
    );
    assert!(
        !key_strings.contains(&hex::encode(b"key02___")),
        "key02 should be excluded"
    );
}

#[test]
fn test_scan_with_lower_and_upper_bound() {
    let db = setup_db();
    let ks_id = db.ks.as_u8() as i64;
    let (engine, mut scope) = open_engine(&db);

    // Live keys: key02___, key03___, key04___
    // lower=key02___, upper=key04___ (exclusive) → only key02___ and key03___
    let lower_hex = hex::encode(b"key02___");
    let upper_hex = hex::encode(b"key04___");

    let snippet = format!(
        r#"
        let keys = [];
        scan({ks_id}, "{lower_hex}", "{upper_hex}", |key, value| {{ keys.push(key); }});
        keys
        "#
    );
    let keys: rhai::Array = engine.eval_with_scope(&mut scope, &snippet).unwrap();
    let key_strings: Vec<String> = keys.into_iter().map(|d| d.cast::<String>()).collect();

    assert_eq!(
        key_strings.len(),
        2,
        "should return exactly key02 and key03"
    );
    assert!(key_strings.contains(&hex::encode(b"key02___")));
    assert!(key_strings.contains(&hex::encode(b"key03___")));
    assert!(
        !key_strings.contains(&hex::encode(b"key04___")),
        "upper bound is exclusive"
    );
}
