// setup_db: 5 ks records + 3 ks2 records = 8 records, 2 removes
let types = [];
db.walk_wal(|entry| { types.push(entry.entry_type); });
let records = 0;
let removes = 0;
for t in types {
    if t == "record" { records += 1; }
    if t == "remove" { removes += 1; }
}
assert_eq(records, 8, "5 ks records + 3 ks2 records");
assert_eq(removes, 2, "2 removes on ks");
