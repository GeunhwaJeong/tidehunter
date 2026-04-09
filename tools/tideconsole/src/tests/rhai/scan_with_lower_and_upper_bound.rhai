let db = setup_db();
// Live keys: key02___, key03___, key04___
// lower=key02___, upper=key04___ (exclusive) → only key02___ and key03___
let keys = [];
db.scan("ks", to_hex("key02___"), to_hex("key04___"), |key, value| { keys.push(key); });
assert_eq(keys.len(), 2, "should return exactly key02 and key03");
assert( keys.contains(to_hex("key02___")), "key02 should be present");
assert( keys.contains(to_hex("key03___")), "key03 should be present");
assert(!keys.contains(to_hex("key04___")), "upper bound is exclusive");
