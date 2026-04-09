let db = setup_db();
// "key02___" was written with value vec![2u8; 16]
let result = db.get("ks", to_hex("key02___"));
assert(result != (), "key should be found");
assert_eq(result, bytes_hex(16, 2), "value should match");
