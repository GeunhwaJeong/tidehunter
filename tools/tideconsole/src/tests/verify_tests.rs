use super::run_rhai_test;

#[test]
fn test_verify_clean_db() {
    run_rhai_test("verify_clean_db.rhai");
}
#[test]
fn test_verify_verified_equals_total() {
    run_rhai_test("verify_verified_equals_total.rhai");
}
