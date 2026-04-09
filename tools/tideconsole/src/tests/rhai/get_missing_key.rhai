let key = to_hex("nokey___");
let result = db.get("ks", key);
assert(result == (), "missing key should return ()");
