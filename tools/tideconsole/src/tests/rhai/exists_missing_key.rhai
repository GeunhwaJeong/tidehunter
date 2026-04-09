let db = setup_db();
assert(!db.exists("ks", to_hex("nokey___")), "missing key should not exist");
