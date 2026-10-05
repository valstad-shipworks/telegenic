#![no_main]

use libfuzzer_sys::fuzz_target;
use telegenic::gige::proto::gvcp::{Ack, Cmd};

fuzz_target!(|data: &[u8]| {
    if let Some(ack) = Ack::parse(data) {
        let _ = ack.register_values().count();
        let _ = ack.pending_ack_timeout_ms();
    }
    let _ = Cmd::parse(data);
});
