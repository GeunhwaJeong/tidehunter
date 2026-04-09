// Shared setup functions injected at the top of every rhai test.
// __db_path__ is injected into scope by the Rust test runner.

/// Standard two-keyspace database.
///   ks  — 5 records (key00___..key04___), then 2 removes (key00___, key01___)  → 3 live keys
///   ks2 — 3 records (key00___..key02___)                                       → 3 live keys
fn setup_db() {
    let db = create_db(__db_path__ + "/std", [
        #{name: "ks",  key_size: 8},
        #{name: "ks2", key_size: 8},
    ]);
    for i in 0..5 {
        db.put("ks", to_hex(`key0${i}___`), bytes_hex(16, i));
    }
    for i in 0..3 {
        db.put("ks2", to_hex(`key0${i}___`), bytes_hex(8, i + 10));
    }
    db.delete("ks", to_hex("key00___"));
    db.delete("ks", to_hex("key01___"));
    db
}

/// Two-keyspace database with a forced control-region snapshot.
///   objects  — 5 records (key00___..key04___)
///   metadata — 3 records (key00___..key02___)
fn setup_cr_db() {
    let db = create_db(__db_path__ + "/cr", [
        #{name: "objects",  key_size: 8},
        #{name: "metadata", key_size: 8},
    ]);
    for i in 0..5 {
        db.put("objects",  to_hex(`key0${i}___`), bytes_hex(16, i));
    }
    for i in 0..3 {
        db.put("metadata", to_hex(`key0${i}___`), bytes_hex(8, i + 10));
    }
    db.force_snapshot();
    db
}
