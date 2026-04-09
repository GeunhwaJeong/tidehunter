// "key00___" was deleted
let key = to_hex("key00___");
let result = db.get("ks", key);
assert(result == (), "deleted key should return ()");
