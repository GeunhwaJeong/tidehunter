let db = setup_cr_db();
// After force_snapshot, at least some cells should have a populated level
let valid = 0;
for ks in db.load_cr()["keyspaces"] {
    for c in ks["cells"] {
        if c["levels"].len() > 0 { valid += 1; }
    }
}
assert(valid > 0, "expected some valid cells after snapshot");
