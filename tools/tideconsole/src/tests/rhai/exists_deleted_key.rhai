let key = to_hex("key00___");
assert(!db.exists("ks", key), "deleted key should not exist");
