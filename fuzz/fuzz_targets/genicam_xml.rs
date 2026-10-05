#![no_main]

use libfuzzer_sys::fuzz_target;
use telegenic::genicam::parse_xml;
use telegenic::genicam::port::PortIo;

/// Every register reads as bytes derived from its address and a seed;
/// reads larger than one sane transfer fail as a real port's would.
struct NoisePort(u64);

impl PortIo for NoisePort {
    fn read(&self, address: u64, buf: &mut [u8]) -> telegenic::Result<()> {
        if buf.len() > 0x1_0000 {
            return Err(telegenic::CameraError::Protocol("oversized read".into()));
        }
        for (i, b) in buf.iter_mut().enumerate() {
            let x = (self.0 ^ address ^ (i as u64) << 32).wrapping_mul(0x9e37_79b9_7f4a_7c15);
            *b = (x >> 56) as u8;
        }
        Ok(())
    }

    fn write(&self, _address: u64, _data: &[u8]) -> telegenic::Result<()> {
        Ok(())
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&seed, text)) = data.split_first() else {
        return;
    };
    let Ok(mut g) = parse_xml(&String::from_utf8_lossy(text)) else {
        return;
    };
    let port = NoisePort(u64::from(seed));
    let names: Vec<String> = g.node_names().map(str::to_string).collect();
    for name in names {
        let Ok(id) = g.lookup(&name) else { continue };
        let _ = g.int_value(id, &port);
        let _ = g.float_value(id, &port);
        let _ = g.bool_value(id, &port);
        let _ = g.string_value(id, &port);
        let _ = g.access_mode(id);
        let _ = g.int_bounds(id, &port);
        let _ = g.float_bounds(id, &port);
        let _ = g.enum_entries(id);
        let _ = g.category_features(id);
    }
});
