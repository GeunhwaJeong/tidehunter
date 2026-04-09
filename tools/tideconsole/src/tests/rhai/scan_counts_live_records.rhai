// ks has 5 records written, 2 removed → 3 live
let count = 0;
db.scan("ks", |key, value| { count += 1; });
assert_eq(count, 3, "3 live records in ks after 2 deletes");
