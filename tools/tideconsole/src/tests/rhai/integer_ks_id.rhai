let db = setup_db();
// "ks" is added first → id = 0
let result = db.get(0, to_hex("key02___"));
assert(result != (), "integer ks ID should resolve correctly");
assert_eq(result, bytes_hex(16, 2), "value should match");
