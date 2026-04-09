// ks_id is injected into scope by the test runner
let key = to_hex("key02___");
let result = db.get(ks_id, key);
assert(result != (), "integer ks ID should resolve correctly");
assert_eq(result, bytes_hex(16, 2), "value should match");
