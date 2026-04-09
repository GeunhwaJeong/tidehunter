let db = setup_db();
let key = to_hex("newkey__");
let value = to_hex("helloworld______"); // 16 bytes
db.put("ks", key, value);
let result = db.get("ks", key);
assert(result != (), "key should exist after put");
assert_eq(result, value, "value should round-trip");
