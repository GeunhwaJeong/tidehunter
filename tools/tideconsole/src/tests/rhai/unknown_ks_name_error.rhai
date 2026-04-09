let key = to_hex("key02___");
let threw = false;
try {
    db.get("nonexistent", key);
} catch(e) {
    threw = true;
    assert(e.to_string().contains("nonexistent"), "error should mention the keyspace name");
}
assert(threw, "unknown keyspace name should return an error");
