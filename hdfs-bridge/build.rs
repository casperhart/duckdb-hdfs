//! Regenerates `src/include/hdfs_bridge.h` (the C header the DuckDB extension
//! compiles against) from this crate's `#[no_mangle]` items, so the header can
//! never drift from the Rust definitions. Configured by `cbindgen.toml`. The
//! generated header is committed: the C++ build only reads it, and a stale
//! checkout still compiles before cargo has run.

use std::path::PathBuf;

fn main() {
    let crate_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap());
    let header = crate_dir.join("../src/include/hdfs_bridge.h");
    println!("cargo:rerun-if-changed=src");
    println!("cargo:rerun-if-changed=cbindgen.toml");
    cbindgen::generate(&crate_dir)
        .expect("cbindgen failed to generate hdfs_bridge.h")
        // Only rewrites the file when the contents changed, so incremental
        // C++ builds aren't invalidated for nothing.
        .write_to_file(header);
}
