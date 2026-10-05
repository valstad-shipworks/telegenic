//! Shared settings for the property suites: a fixed seed, so a failure
//! replays on every run and machine, and no regression files written into
//! the source tree. Raise the case count for a deeper local run with
//! `PROPTEST_CASES`, which overrides the per-suite default.

#![allow(dead_code)]

use proptest::test_runner::{Config, RngSeed};

pub const SEED: u64 = 0x7e1e_9e41_c0de;

pub fn config(cases: u32) -> Config {
    let cases = std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(cases);
    Config {
        cases,
        rng_seed: RngSeed::Fixed(SEED),
        failure_persistence: None,
        ..Config::default()
    }
}

pub fn flip_bit(bytes: &[u8], bit: usize) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[bit / 8] ^= 1 << (bit % 8);
    out
}
