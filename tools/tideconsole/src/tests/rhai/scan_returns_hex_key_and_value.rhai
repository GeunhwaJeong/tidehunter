// ks2 has 3 records with 8-byte values → 16 hex chars each
let lens = [];
db.scan("ks2", |key, value| { lens.push(value.len()); });
assert_eq(lens.len(), 3, "3 records in ks2");
for len in lens {
    assert_eq(len, 16, "8 bytes encodes to 16 hex chars");
}
