// force_snapshot so all data is indexed
db.force_snapshot();

let result = db.analyze_ks("objects");

// Structural checks
let required_fields = [
    "total_index_entries", "total_keys", "total_payload_bytes",
    "avg_payload_bytes", "min_payload_bytes", "max_payload_bytes",
    "p50", "p90", "p99",
];
for field in required_fields {
    assert(field in result, `missing field: ${field}`);
}

// setup_db_with_cr writes 5 keys to "objects"
assert_eq(result["total_keys"], 5, "expected 5 keys in objects keyspace");
assert(result["total_payload_bytes"] > 0, "total_payload_bytes should be positive");
assert(result["min_payload_bytes"]   > 0, "min_payload_bytes should be positive");
assert(result["max_payload_bytes"]  >= result["min_payload_bytes"], "max >= min");
