let db = setup_db();
// 5 ks writes, 2 ks deletes, 3 ks2 writes → 3 + 3 = 6 live keys
let result = db.verify();
assert_eq(result["total"],        6, "6 live keys");
assert_eq(result["missing"],      0, "no missing keys");
assert_eq(result["errors"],       0, "no errors");
assert_eq(result["iter_missing"], 0, "no iter_missing");
assert_eq(result["iter_errors"],  0, "no iter_errors");
assert(result["ok"], "verify should pass");
