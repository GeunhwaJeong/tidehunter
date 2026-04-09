let db = setup_db();
assert(db.exists("ks", to_hex("key03___")), "key03 should exist");
