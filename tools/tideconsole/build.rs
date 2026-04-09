use std::path::Path;
use std::{env, fs};

fn main() {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR").unwrap();
    let rhai_dir = Path::new(&manifest_dir).join("src/tests/rhai");

    // Re-run whenever a .rhai file is added or removed.
    println!("cargo:rerun-if-changed={}", rhai_dir.display());

    let mut stems: Vec<String> = Vec::new();

    for entry in fs::read_dir(&rhai_dir).expect("cannot read src/tests/rhai/") {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if !name.ends_with(".rhai") {
            continue;
        }
        if name == "__common__.rhai" {
            continue;
        }
        if name.starts_with('_') {
            panic!(
                "Unexpected .rhai file starting with '_': {name}. \
                 Only __common__.rhai is reserved; rename or remove this file."
            );
        }
        stems.push(name.strip_suffix(".rhai").unwrap().to_string());
    }

    stems.sort();

    let mut out = String::new();
    for stem in &stems {
        out.push_str(&format!(
            "#[test]\nfn test_{stem}() {{\n    super::run_rhai_test(\"{stem}.rhai\");\n}}\n\n"
        ));
    }

    let out_dir = env::var("OUT_DIR").unwrap();
    fs::write(Path::new(&out_dir).join("rhai_tests_generated.rs"), out).unwrap();
}
