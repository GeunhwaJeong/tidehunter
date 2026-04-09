let db = setup_db();
let result = db.get("ks", to_hex("nokey___"));
assert(result == (), "missing key should return ()");
