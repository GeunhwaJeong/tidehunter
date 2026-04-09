// After force_rebuild_control_region, at least some cells should have valid offsets
let cr = db.load_cr();
let valid = 0;
for ks in cr["keyspaces"] {
    for c in ks["cells"] {
        if c["offset"] >= 0 { valid += 1; }
    }
}
assert(valid > 0, "expected some valid cells after snapshot");
