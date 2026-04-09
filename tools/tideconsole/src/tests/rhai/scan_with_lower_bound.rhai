// Live keys in ks: key02___, key03___, key04___
// lower bound = key03___ → should return key03___ and key04___
let lower = to_hex("key03___");
let keys = [];
db.scan("ks", lower, |key, value| { keys.push(key); });
assert(keys.contains(to_hex("key03___")), "key03 should be included");
assert(keys.contains(to_hex("key04___")), "key04 should be included");
assert(!keys.contains(to_hex("key02___")), "key02 should be excluded by lower bound");
