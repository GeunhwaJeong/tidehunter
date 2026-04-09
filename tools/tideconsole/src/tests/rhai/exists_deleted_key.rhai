let db = setup_db();
assert(!db.exists("ks", to_hex("key00___")), "deleted key should not exist");
