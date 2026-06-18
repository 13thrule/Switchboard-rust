use std::fs;
use std::path::PathBuf;

#[test]
fn demo_page_contains_controls() {
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    // repo root is two levels above the crate manifest dir
    let demo_path: PathBuf = [manifest.as_str(), "..", "..", "demo", "index.html"].iter().collect();
    let demo_path = demo_path.canonicalize().expect("demo/index.html should exist");
    let contents = fs::read_to_string(demo_path).expect("reading demo file");
    assert!(contents.contains("Connect"), "demo page should include Connect button");
    assert!(contents.contains("Subscribe"), "demo page should include Subscribe button");
    assert!(contents.contains("Publish"), "demo page should include Publish button");
}
