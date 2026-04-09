let key = to_hex("nokey___");
assert(!db.exists("ks", key), "missing key should not exist");
