let db = setup_db();
// "key00___" was deleted
let result = db.get("ks", to_hex("key00___"));
assert(result == (), "deleted key should return ()");
