// db.load_index(-1) should fail because -1 means "no index"
let threw = false;
try {
    db.load_index(-1);
} catch(e) {
    threw = true;
    assert(e.to_string().contains("negative"), "error should mention negative offset");
}
assert(threw, "db.load_index(-1) should have failed");
