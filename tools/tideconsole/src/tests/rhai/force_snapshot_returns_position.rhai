let db = setup_cr_db();
let pos = db.force_snapshot();
assert(pos >= 0, "force_snapshot should return a non-negative WAL position");

// CR is still readable after the snapshot
assert(db.load_cr()["last_position"] >= 0, "CR should be readable after snapshot");
