#![no_main]

use libfuzzer_sys::fuzz_target;
use telegenic::gige::proto::gvsp::{GvspView, ImageLeader};

fuzz_target!(|data: &[u8]| {
    if let Some(view) = GvspView::parse(data) {
        let _ = ImageLeader::parse(view.data);
    }
    let _ = ImageLeader::parse(data);
});
