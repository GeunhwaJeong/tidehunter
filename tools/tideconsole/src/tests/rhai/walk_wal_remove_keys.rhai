// ks_id is injected into scope by the test runner
// key00___ and key01___ were deleted from ks
let remove_keys = [];
db.walk_wal(|entry| {
    if entry.entry_type == "remove" && entry.keyspace == ks_id {
        remove_keys.push(entry.key);
    }
});
assert_eq(remove_keys.len(), 2, "2 removes on ks");
assert(remove_keys.contains(to_hex("key00___")), "key00 should be in removes");
assert(remove_keys.contains(to_hex("key01___")), "key01 should be in removes");
