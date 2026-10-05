//! The library reads time and waits only through libc calls an interposing
//! simulation can see. A cycle counter, inline assembly, a TSC clock crate or
//! a raw clock/futex syscall would read or wait on real time behind its back.

use std::fs;
use std::path::{Path, PathBuf};

const INVISIBLE: &[&str] = &[
    "rdtsc",
    "asm!",
    "quanta::",
    "minstant::",
    "std::arch::",
    "core::arch::",
    "SYS_clock_",
    "SYS_futex",
    "SYS_nanosleep",
    "SYS_gettimeofday",
];

fn sources(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            sources(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

fn invisible_waits(text: &str) -> Vec<(usize, &'static str)> {
    text.lines()
        .enumerate()
        .filter(|(_, l)| !l.trim_start().starts_with("//"))
        .flat_map(|(i, l)| {
            INVISIBLE
                .iter()
                .filter(move |p| l.contains(*p))
                .map(move |p| (i + 1, *p))
        })
        .collect()
}

#[test]
fn library_time_and_waits_are_visible_to_interposition() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    sources(&root, &mut files);
    let violations: Vec<String> = files
        .iter()
        .flat_map(|file| {
            let text = fs::read_to_string(file).unwrap();
            invisible_waits(&text)
                .into_iter()
                .map(|(n, p)| format!("{}:{n}: `{p}`", file.display()))
                .collect::<Vec<_>>()
        })
        .collect();
    assert!(
        violations.is_empty(),
        "time read or wait an interposing simulation cannot see:\n{}",
        violations.join("\n")
    );
}

#[test]
fn the_lint_flags_code_and_skips_comments() {
    let text = "let t = unsafe { core::arch::x86_64::_rdtsc() };\n\
                // rdtsc in a comment\n\
                libc::syscall(libc::SYS_futex, addr, op);\n";
    assert_eq!(
        invisible_waits(text),
        vec![(1, "rdtsc"), (1, "core::arch::"), (3, "SYS_futex")]
    );
}
