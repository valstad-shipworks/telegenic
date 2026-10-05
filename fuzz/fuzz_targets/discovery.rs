#![no_main]

use libfuzzer_sys::fuzz_target;
use telegenic::gige::DeviceInfo;
use telegenic::gige::proto::gvcp::Ack;

fuzz_target!(|data: &[u8]| {
    if let Some(ack) = Ack::parse(data) {
        let _ = DeviceInfo::parse(ack.payload);
    }
    let _ = DeviceInfo::parse(data);
});
