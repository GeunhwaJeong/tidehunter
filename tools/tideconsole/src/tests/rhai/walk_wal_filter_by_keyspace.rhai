// ks_id and ks2_id are injected into scope by the test runner
let ks_records = 0;
let ks2_records = 0;
db.walk_wal(|entry| {
    if entry.entry_type == "record" {
        if entry.keyspace == ks_id  { ks_records += 1; }
        if entry.keyspace == ks2_id { ks2_records += 1; }
    }
});
assert_eq(ks_records,  5, "5 records in ks");
assert_eq(ks2_records, 3, "3 records in ks2");
