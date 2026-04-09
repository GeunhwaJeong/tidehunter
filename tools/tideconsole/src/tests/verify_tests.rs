use super::{run_rhai_test, setup_db};

#[test]
fn test_verify_clean_db() {
    run_rhai_test(&setup_db(), "verify_clean_db.rhai");
}

#[test]
fn test_verify_verified_equals_total() {
    run_rhai_test(&setup_db(), "verify_verified_equals_total.rhai");
}
