#![no_main]

use libfuzzer_sys::fuzz_target;
use telegenic::genicam::XmlUrl;

fuzz_target!(|data: &[u8]| {
    if let Ok(url) = XmlUrl::parse(&String::from_utf8_lossy(data)) {
        let _ = url.is_zip();
    }
});
