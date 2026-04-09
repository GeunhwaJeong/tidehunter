// "key02___" was written with value vec![2u8; 16]
let key = to_hex("key02___");
let expected = bytes_hex(16, 2);
let result = db.get("ks", key);
assert(result != (), "key should be found");
assert_eq(result, expected, "value should match");
