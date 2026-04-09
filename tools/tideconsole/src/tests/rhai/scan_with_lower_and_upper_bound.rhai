// Live keys: key02___, key03___, key04___
// lower=key02___, upper=key04___ (exclusive) → only key02___ and key03___
let lower = to_hex("key02___");
let upper = to_hex("key04___");
let keys = [];
db.scan("ks", lower, upper, |key, value| { keys.push(key); });
assert_eq(keys.len(), 2, "should return exactly key02 and key03");
assert(keys.contains(to_hex("key02___")), "key02 should be present");
assert(keys.contains(to_hex("key03___")), "key03 should be present");
assert(!keys.contains(to_hex("key04___")), "upper bound is exclusive");
