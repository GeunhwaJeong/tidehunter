use super::run_rhai_test;

#[test]
fn test_get_existing_key() {
    run_rhai_test("get_existing_key.rhai");
}
#[test]
fn test_get_deleted_key_returns_unit() {
    run_rhai_test("get_deleted_key.rhai");
}
#[test]
fn test_get_missing_key_returns_unit() {
    run_rhai_test("get_missing_key.rhai");
}
#[test]
fn test_exists_present_key() {
    run_rhai_test("exists_present_key.rhai");
}
#[test]
fn test_exists_deleted_key() {
    run_rhai_test("exists_deleted_key.rhai");
}
#[test]
fn test_exists_missing_key() {
    run_rhai_test("exists_missing_key.rhai");
}
#[test]
fn test_put_and_get() {
    run_rhai_test("put_and_get.rhai");
}
#[test]
fn test_delete_existing_key() {
    run_rhai_test("delete_existing_key.rhai");
}
#[test]
fn test_scan_counts_live_records() {
    run_rhai_test("scan_counts_live_records.rhai");
}
#[test]
fn test_scan_returns_hex_key_and_value() {
    run_rhai_test("scan_returns_hex_key_and_value.rhai");
}
#[test]
fn test_scan_with_lower_bound() {
    run_rhai_test("scan_with_lower_bound.rhai");
}
#[test]
fn test_scan_with_lower_and_upper_bound() {
    run_rhai_test("scan_with_lower_and_upper_bound.rhai");
}
#[test]
fn test_integer_ks_id_still_works() {
    run_rhai_test("integer_ks_id.rhai");
}
#[test]
fn test_unknown_ks_name_returns_error() {
    run_rhai_test("unknown_ks_name_error.rhai");
}
