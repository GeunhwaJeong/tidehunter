let db = setup_db();
let result = db.verify();
let total = result["total"];
assert_eq(result["verified"],      total, "verified should equal total");
assert_eq(result["iter_verified"], total, "iter_verified should equal total");
