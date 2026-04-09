let db = setup_db();
// 5 ks records + 3 ks2 records = 8 records, 2 removes
let records = 0;
let removes = 0;
db.walk_wal(|entry| {
    if entry.entry_type == "record" { records += 1; }
    if entry.entry_type == "remove" { removes += 1; }
});
assert_eq(records, 8, "5 ks records + 3 ks2 records");
assert_eq(removes, 2, "2 removes on ks");
