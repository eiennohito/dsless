use std::io::Write;
use std::path::Path;
use std::process::Command;

fn dsless_bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_dsless"))
}

fn write_jsonl(dir: &Path, name: &str, lines: &[&str]) -> std::path::PathBuf {
    let path = dir.join(name);
    let mut f = std::fs::File::create(&path).unwrap();
    for line in lines {
        writeln!(f, "{}", line).unwrap();
    }
    path
}

fn run(args: &[&str]) -> (String, bool) {
    let output = dsless_bin()
        .args(args)
        .output()
        .expect("failed to run dsless");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    (stdout, output.status.success())
}

#[test]
fn flat_jsonl_renders_table() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_jsonl(
        dir.path(),
        "data.jsonl",
        &[r#"{"name":"alice","age":30}"#, r#"{"name":"bob","age":25}"#],
    );

    let (out, ok) = run(&[path.to_str().unwrap()]);
    assert!(ok, "dsless failed");
    assert!(out.contains("alice"), "missing alice in:\n{out}");
    assert!(out.contains("bob"), "missing bob in:\n{out}");
    assert!(out.contains("30"), "missing 30 in:\n{out}");
    assert!(
        out.contains("Total: 2 rows"),
        "missing row count in:\n{out}"
    );
}

#[test]
fn nested_jsonl_renders_vertical() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_jsonl(
        dir.path(),
        "data.jsonl",
        &[
            r#"{"id":1,"info":{"name":"alice","scores":[90,85]}}"#,
            r#"{"id":2,"info":{"name":"bob","scores":[78]}}"#,
        ],
    );

    let (out, ok) = run(&[path.to_str().unwrap()]);
    assert!(ok, "dsless failed");
    assert!(out.contains("Row 0"), "missing Row 0 in:\n{out}");
    assert!(out.contains("alice"), "missing alice in:\n{out}");
    assert!(out.contains("bob"), "missing bob in:\n{out}");
}

#[test]
fn max_rows_flag_limits_output() {
    let dir = tempfile::tempdir().unwrap();
    let mut lines: Vec<String> = Vec::new();
    for i in 0..50 {
        lines.push(format!(r#"{{"i":{i}}}"#));
    }
    let strs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
    let path = write_jsonl(dir.path(), "data.jsonl", &strs);

    let (out, ok) = run(&["-n", "5", path.to_str().unwrap()]);
    assert!(ok, "dsless failed");
    assert!(
        out.contains("stopped at 5 rows"),
        "missing truncation message in:\n{out}"
    );
    assert!(
        !out.contains("Total: 50 rows"),
        "should not show full count in:\n{out}"
    );
}

#[test]
fn jsonl_directory_combines_files() {
    let dir = tempfile::tempdir().unwrap();
    write_jsonl(dir.path(), "part1.jsonl", &[r#"{"v":"a"}"#, r#"{"v":"b"}"#]);
    write_jsonl(dir.path(), "part2.jsonl", &[r#"{"v":"c"}"#]);

    let (out, ok) = run(&[dir.path().to_str().unwrap()]);
    assert!(ok, "dsless failed");
    assert!(out.contains("a"), "missing 'a' in:\n{out}");
    assert!(out.contains("c"), "missing 'c' in:\n{out}");
    assert!(
        out.contains("Total: 3 rows"),
        "missing row count in:\n{out}"
    );
}

#[test]
fn ndjson_extension_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_jsonl(dir.path(), "data.ndjson", &[r#"{"x":1}"#]);

    let (out, ok) = run(&[path.to_str().unwrap()]);
    assert!(ok, "dsless failed");
    assert!(out.contains("Total: 1 row"), "missing row count in:\n{out}");
}

#[test]
fn unsupported_extension_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_jsonl(dir.path(), "data.txt", &[r#"{"x":1}"#]);

    let (_, ok) = run(&[path.to_str().unwrap()]);
    assert!(!ok, "should fail on .txt extension");
}

#[test]
fn empty_jsonl_file_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_jsonl(dir.path(), "empty.jsonl", &[]);

    let (_, ok) = run(&[path.to_str().unwrap()]);
    assert!(!ok, "should fail on empty file");
}

#[test]
fn nullable_fields_render() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_jsonl(
        dir.path(),
        "data.jsonl",
        &[
            r#"{"a":1,"b":"yes"}"#,
            r#"{"a":2}"#,
            r#"{"a":3,"b":"back"}"#,
        ],
    );

    let (out, ok) = run(&[path.to_str().unwrap()]);
    assert!(ok, "dsless failed");
    assert!(out.contains("yes"), "missing 'yes' in:\n{out}");
    assert!(out.contains("back"), "missing 'back' in:\n{out}");
    assert!(
        out.contains("Total: 3 rows"),
        "missing row count in:\n{out}"
    );
}
