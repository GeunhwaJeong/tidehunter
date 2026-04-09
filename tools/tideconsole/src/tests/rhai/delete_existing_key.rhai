let key = to_hex("key04___");
let before = db.exists("ks", key);
db.delete("ks", key);
let after = db.exists("ks", key);
assert(before, "key should exist before delete");
assert(!after, "key should be gone after delete");
