let db = setup_db();
let threw = false;
try {
    db.get("nonexistent", to_hex("key02___"));
} catch(e) {
    threw = true;
    assert(e.to_string().contains("nonexistent"), "error should mention the keyspace name");
}
assert(threw, "unknown keyspace name should return an error");
