// ks_id is injected into scope by the test runner
// First record in ks is "key00___" with value vec![0u8; 16]
let found = #{};
db.walk_wal(|entry| {
    if entry.entry_type == "record" && entry.keyspace == ks_id && found.is_empty() {
        found = #{
            "key_len":          entry.key.len(),       // 8 bytes → 16 hex chars
            "value_len":        entry.value_len,       // 16 bytes
            "value_hex_len":    entry.value.len(),     // 16 bytes → 32 hex chars
            "raw_size_pos":     entry.raw_size > 0,
            "position_nonneg":  entry.position >= 0,
        };
    }
});
assert_eq(found["key_len"],       16, "key: 8 bytes → 16 hex chars");
assert_eq(found["value_len"],     16, "value: 16 bytes");
assert_eq(found["value_hex_len"], 32, "value hex: 16 bytes → 32 hex chars");
assert(found["raw_size_pos"],     "raw_size should be positive");
assert(found["position_nonneg"],  "position should be non-negative");
