//! Property tests for the device description path: the XML URL register,
//! the zipped file, the XML itself, the formula evaluator, and the node
//! graph evaluated against arbitrary register contents. The description is
//! the camera's own file, read over the wire; a corrupted byte there that
//! decodes silently becomes a wrong feature model.

mod fuzzing;

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use proptest::prelude::*;
use telegenic::genicam::evaluator::{Expr, Value};
use telegenic::genicam::port::{MockPort, PortIo};
use telegenic::genicam::{Genicam, XmlUrl, parse_xml, unzip};

struct Tracking;

thread_local! {
    static LARGEST: Cell<usize> = const { Cell::new(0) };
}

fn note(size: usize) {
    let _ = LARGEST.try_with(|c| c.set(c.get().max(size)));
}

// SAFETY: forwards every call to the system allocator unchanged.
unsafe impl GlobalAlloc for Tracking {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note(new_size);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Tracking = Tracking;

/// Runs `f` and reports the largest single allocation it made on this
/// thread.
fn largest_allocation<R>(f: impl FnOnce() -> R) -> (R, usize) {
    LARGEST.with(|c| c.set(0));
    let out = f();
    (out, LARGEST.with(Cell::get))
}

const ALLOCATION_BOUND: usize = 64 << 20;

/// Register reads return bytes derived from the address and a seed, so
/// every address holds some arbitrary value. Reads too large for one
/// sensible transfer fail, as a real port's would.
struct NoisePort(u64);

impl PortIo for NoisePort {
    fn read(&self, address: u64, buf: &mut [u8]) -> telegenic::Result<()> {
        if buf.len() > 0x1_0000 {
            return Err(telegenic::CameraError::Protocol("oversized read".into()));
        }
        for (i, b) in buf.iter_mut().enumerate() {
            let mut x = self.0 ^ address.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (i as u64) << 32;
            x ^= x >> 33;
            x = x.wrapping_mul(0xff51_afd7_ed55_8ccd);
            x ^= x >> 29;
            *b = x as u8;
        }
        Ok(())
    }

    fn write(&self, _address: u64, _data: &[u8]) -> telegenic::Result<()> {
        Ok(())
    }
}

fn vendor_xml(name: &str) -> String {
    std::fs::read_to_string(format!("{}/tests/data/{name}", env!("CARGO_MANIFEST_DIR")))
        .expect("read vendor xml")
}

/// Reads every node every way the public API allows. Errors are fine;
/// the evaluator must answer each call.
fn evaluate_everything(g: &mut Genicam, port: &dyn PortIo) {
    let names: Vec<String> = g.node_names().map(str::to_string).collect();
    for name in names {
        let Ok(id) = g.lookup(&name) else { continue };
        let _ = g.kind_of(id);
        let _ = g.category_features(id);
        let _ = g.enum_entries(id);
        let _ = g.int_value(id, port);
        let _ = g.float_value(id, port);
        let _ = g.bool_value(id, port);
        let _ = g.string_value(id, port);
        let _ = g.int_bounds(id, port);
        let _ = g.int_increment(id, port);
        let _ = g.float_bounds(id, port);
        let _ = g.access_mode(id);
    }
}

fn wrap_xml(body: &str) -> String {
    format!(
        r#"<?xml version="1.0"?><RegisterDescription ModelName="T" VendorName="T">{body}<Port Name="Device"/></RegisterDescription>"#
    )
}

fn masked_reg(length: usize, big: bool, signed: bool, lsb: u32, msb: u32) -> String {
    format!(
        "<MaskedIntReg Name=\"R\"><Address>0x10</Address><Length>{length}</Length>\
         <AccessMode>RW</AccessMode><pPort>Device</pPort><Cachable>NoCache</Cachable>\
         <Sign>{}</Sign><Endianess>{}</Endianess><LSB>{lsb}</LSB><MSB>{msb}</MSB></MaskedIntReg>",
        if signed { "Signed" } else { "Unsigned" },
        if big { "BigEndian" } else { "LittleEndian" },
    )
}

fn register_value(bytes: &[u8], big: bool) -> u64 {
    let mut v = 0u64;
    for (i, &b) in bytes.iter().enumerate() {
        let shift = if big {
            8 * (bytes.len() - 1 - i)
        } else {
            8 * i
        };
        v |= u64::from(b) << shift;
    }
    v
}

fn field(value: u64, lo: u32, hi: u32, signed: bool) -> i64 {
    let width = hi - lo + 1;
    let raw = if width == 64 {
        value
    } else {
        (value >> lo) & ((1u64 << width) - 1)
    };
    if signed && width < 64 && raw >> (width - 1) & 1 == 1 {
        (raw | !((1u64 << width) - 1)) as i64
    } else {
        raw as i64
    }
}

/// A conventional bit range `lo..=hi` (bit 0 the least significant) of a
/// register `length` bytes long, together with how the XML spells it:
/// little-endian registers number bits from the LSB, big-endian ones from
/// the MSB.
fn bit_range() -> impl Strategy<Value = (usize, bool, u32, u32)> {
    (prop::sample::select(vec![1usize, 2, 4, 8]), any::<bool>()).prop_flat_map(|(len, big)| {
        let bits = 8 * len as u32;
        (0..bits)
            .prop_flat_map(move |lo| (Just(lo), lo..bits))
            .prop_map(move |(lo, hi)| (len, big, lo, hi))
    })
}

fn xml_bits(len: usize, big: bool, lo: u32, hi: u32) -> (u32, u32) {
    let bits = 8 * len as u32;
    if big {
        (bits - 1 - lo, bits - 1 - hi)
    } else {
        (lo, hi)
    }
}

#[derive(Debug, Clone)]
enum E {
    Lit(i64),
    Var(usize),
    Neg(Box<E>),
    Not(Box<E>),
    Bin(&'static str, Box<E>, Box<E>),
    If(Box<E>, Box<E>, Box<E>),
}

const VARS: [&str; 3] = ["X0", "X1", "X2"];
const BINARY: [&str; 18] = [
    "+", "-", "*", "/", "%", "&", "|", "^", "<<", ">>", "=", "<>", "<", ">", "<=", ">=", "&&", "||",
];

fn expr() -> impl Strategy<Value = E> {
    let leaf = prop_oneof![
        prop_oneof![0i64..64, 0i64..=i64::MAX].prop_map(E::Lit),
        (0..VARS.len()).prop_map(E::Var),
    ];
    leaf.prop_recursive(5, 48, 3, |inner| {
        prop_oneof![
            inner.clone().prop_map(|e| E::Neg(Box::new(e))),
            inner.clone().prop_map(|e| E::Not(Box::new(e))),
            (
                prop::sample::select(&BINARY[..]),
                inner.clone(),
                inner.clone()
            )
                .prop_map(|(op, a, b)| E::Bin(op, Box::new(a), Box::new(b))),
            (inner.clone(), inner.clone(), inner).prop_map(|(c, a, b)| E::If(
                Box::new(c),
                Box::new(a),
                Box::new(b)
            )),
        ]
    })
}

fn print(e: &E) -> String {
    match e {
        E::Lit(v) => v.to_string(),
        E::Var(i) => VARS[*i].to_string(),
        E::Neg(a) => format!("(-{})", print(a)),
        E::Not(a) => format!("(~{})", print(a)),
        E::Bin(op, a, b) => format!("({} {op} {})", print(a), print(b)),
        E::If(c, a, b) => format!("({} ? {} : {})", print(c), print(a), print(b)),
    }
}

/// C-like 64-bit integer semantics, wrapping on overflow; `None` where the
/// formula has no value (division by zero).
fn reference(e: &E, vars: &[i64]) -> Option<i64> {
    Some(match e {
        E::Lit(v) => *v,
        E::Var(i) => vars[*i],
        E::Neg(a) => reference(a, vars)?.wrapping_neg(),
        E::Not(a) => !reference(a, vars)?,
        E::If(c, a, b) => {
            let (c, a, b) = (
                reference(c, vars)?,
                reference(a, vars)?,
                reference(b, vars)?,
            );
            if c != 0 { a } else { b }
        }
        E::Bin(op, a, b) => {
            let (a, b) = (reference(a, vars)?, reference(b, vars)?);
            match *op {
                "+" => a.wrapping_add(b),
                "-" => a.wrapping_sub(b),
                "*" => a.wrapping_mul(b),
                "/" => {
                    if b == 0 {
                        return None;
                    }
                    a.wrapping_div(b)
                }
                "%" => {
                    if b == 0 {
                        return None;
                    }
                    a.wrapping_rem(b)
                }
                "&" => a & b,
                "|" => a | b,
                "^" => a ^ b,
                "<<" => a.wrapping_shl(b as u32),
                ">>" => a.wrapping_shr(b as u32),
                "=" => i64::from(a == b),
                "<>" => i64::from(a != b),
                "<" => i64::from(a < b),
                ">" => i64::from(a > b),
                "<=" => i64::from(a <= b),
                ">=" => i64::from(a >= b),
                "&&" => i64::from(a != 0 && b != 0),
                "||" => i64::from(a != 0 || b != 0),
                _ => unreachable!(),
            }
        }
    })
}

fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xedb8_8320
            } else {
                crc >> 1
            };
        }
    }
    !crc
}

fn zip(name: &str, content: &[u8], deflate: bool) -> Vec<u8> {
    let (method, data) = if deflate {
        (8u16, miniz_oxide::deflate::compress_to_vec(content, 6))
    } else {
        (0u16, content.to_vec())
    };
    let header = |sig: u32, central: bool| {
        let mut h = sig.to_le_bytes().to_vec();
        if central {
            h.extend_from_slice(&20u16.to_le_bytes());
        }
        h.extend_from_slice(&20u16.to_le_bytes());
        h.extend_from_slice(&0u16.to_le_bytes());
        h.extend_from_slice(&method.to_le_bytes());
        h.extend_from_slice(&[0u8; 4]);
        h.extend_from_slice(&crc32(content).to_le_bytes());
        h.extend_from_slice(&(data.len() as u32).to_le_bytes());
        h.extend_from_slice(&(content.len() as u32).to_le_bytes());
        h.extend_from_slice(&(name.len() as u16).to_le_bytes());
        h
    };
    let mut z = header(0x0403_4b50, false);
    z.extend_from_slice(&0u16.to_le_bytes());
    z.extend_from_slice(name.as_bytes());
    z.extend_from_slice(&data);
    let central = z.len();
    z.extend_from_slice(&header(0x0201_4b50, true));
    z.extend_from_slice(&[0u8; 16]);
    z.extend_from_slice(name.as_bytes());
    let central_size = z.len() - central;
    z.extend_from_slice(&0x0605_4b50u32.to_le_bytes());
    z.extend_from_slice(&[0u8; 4]);
    z.extend_from_slice(&1u16.to_le_bytes());
    z.extend_from_slice(&1u16.to_le_bytes());
    z.extend_from_slice(&(central_size as u32).to_le_bytes());
    z.extend_from_slice(&(central as u32).to_le_bytes());
    z.extend_from_slice(&0u16.to_le_bytes());
    z
}

fn mutate(text: &str, edits: &[(prop::sample::Index, u8, u8)]) -> String {
    let mut bytes = text.as_bytes().to_vec();
    for (at, op, byte) in edits {
        let i = at.index(bytes.len());
        match op % 3 {
            0 => bytes[i] = *byte,
            1 => {
                bytes.remove(i);
            }
            _ => bytes.insert(i, *byte),
        }
    }
    String::from_utf8_lossy(&bytes).into_owned()
}

proptest! {
    #![proptest_config(fuzzing::config(1024))]

    #[test]
    fn arbitrary_urls_parse_without_panicking(s in "\\PC{0,80}", raw in prop::collection::vec(any::<u8>(), 0..80)) {
        let _ = XmlUrl::parse(&s);
        let _ = XmlUrl::parse(&String::from_utf8_lossy(&raw));
    }

    /// The URL register's three forms decode to exactly what was written,
    /// whatever the scheme's case and however much NUL padding follows.
    #[test]
    fn urls_roundtrip(
        name in "[A-Za-z0-9_.-]{1,40}",
        zipped in any::<bool>(),
        address in any::<u64>(),
        size in any::<u32>(),
        upper in prop::collection::vec(any::<bool>(), 6),
        pad in 0usize..64,
        path in "(/[A-Za-z0-9_.-]{1,10}){1,4}",
    ) {
        let scheme: String = "local:"
            .chars()
            .zip(&upper)
            .map(|(c, &u)| if u { c.to_ascii_uppercase() } else { c })
            .collect();
        let filename = if zipped { format!("{name}.zip") } else { format!("{name}.xml") };
        let text = format!("{scheme}{filename};{address:X};{size:x}{}", "\0".repeat(pad));
        let url = XmlUrl::parse(&text).expect("a well-formed Local URL parses");
        prop_assert_eq!(
            &url,
            &XmlUrl::Local { filename: filename.clone(), address, size: size as usize }
        );
        prop_assert_eq!(url.is_zip(), zipped);
        prop_assert_eq!(XmlUrl::parse(&format!("File://{path}")).unwrap(), XmlUrl::File(path.clone()));
        let http = format!("http://cam{path}");
        prop_assert_eq!(XmlUrl::parse(&http).unwrap(), XmlUrl::Http(http.clone()));
    }

    #[test]
    fn arbitrary_bytes_unzip_without_panicking(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = unzip(&bytes);
    }

    /// Stored and deflated archives yield their content exactly.
    #[test]
    fn zips_roundtrip(content in prop::collection::vec(any::<u8>(), 0..4096), deflate in any::<bool>()) {
        let z = zip("cam.xml", &content, deflate);
        prop_assert_eq!(unzip(&z).expect("a valid archive unzips"), content);
    }

    /// Header fields overwritten with arbitrary values — offsets, sizes,
    /// counts, methods — are rejected or decoded without panicking, and
    /// never make the reader allocate past its 64 MiB entry bound.
    #[test]
    fn zip_headers_with_arbitrary_fields_stay_bounded(
        content in prop::collection::vec(any::<u8>(), 0..512),
        deflate in any::<bool>(),
        writes in prop::collection::vec((any::<prop::sample::Index>(), any::<u32>(), 1usize..=4), 1..6),
    ) {
        let mut z = zip("cam.xml", &content, deflate);
        for (at, value, width) in &writes {
            let i = at.index(z.len().saturating_sub(*width));
            let n = (*width).min(z.len() - i);
            z[i..i + n].copy_from_slice(&value.to_le_bytes()[..n]);
        }
        let (_, largest) = largest_allocation(|| unzip(&z));
        prop_assert!(largest <= ALLOCATION_BOUND + 4096, "allocated {} bytes", largest);
    }

    /// Cutting an archive anywhere loses its directory: rejected.
    #[test]
    fn truncated_zips_are_rejected(
        content in prop::collection::vec(any::<u8>(), 1..1024),
        deflate in any::<bool>(),
        cut in any::<prop::sample::Index>(),
    ) {
        let z = zip("cam.xml", &content, deflate);
        let keep = cut.index(z.len());
        prop_assert!(unzip(&z[..keep]).is_err());
    }

    #[test]
    fn arbitrary_xml_parses_without_panicking(s in "\\PC{0,400}") {
        if let Ok(mut g) = parse_xml(&s) {
            evaluate_everything(&mut g, &NoisePort(1));
        }
        let tagged = format!("<RegisterDescription>{s}</RegisterDescription>");
        if let Ok(mut g) = parse_xml(&tagged) {
            evaluate_everything(&mut g, &NoisePort(2));
        }
    }

    /// Arbitrary formulas compile or are rejected, and a compiled formula
    /// evaluates against any variable values without panicking.
    #[test]
    fn arbitrary_formulas_parse_and_evaluate_without_panicking(
        src in "[-+*/%&|^~<>=?:()., 0-9A-Za-z_]{0,60}",
        any_text in "\\PC{0,40}",
        ints in prop::collection::vec(any::<i64>(), 8),
        floats in prop::collection::vec(any::<f64>(), 8),
    ) {
        for text in [&src, &any_text] {
            if let Ok(expr) = Expr::parse(text) {
                let n = expr.variables().len();
                let as_ints: Vec<Value> = (0..n).map(|i| Value::I(ints[i % 8])).collect();
                let as_floats: Vec<Value> = (0..n).map(|i| Value::F(floats[i % 8])).collect();
                let _ = expr.eval(&as_ints);
                let _ = expr.eval(&as_floats);
                let _ = expr.eval(&[]);
            }
        }
    }

    /// Integer formulas evaluate with C's 64-bit semantics: any expression
    /// tree printed and compiled gives the reference value for any
    /// variable values, or an error exactly where it divides by zero.
    #[test]
    fn integer_formulas_match_reference_semantics(e in expr(), vars in prop::array::uniform3(any::<i64>())) {
        let text = print(&e);
        let compiled = Expr::parse(&text).unwrap_or_else(|err| panic!("{text}: {err}"));
        let values: Vec<Value> = compiled
            .variables()
            .iter()
            .map(|name| Value::I(vars[VARS.iter().position(|v| v == name).unwrap()]))
            .collect();
        match (compiled.eval(&values), reference(&e, &vars)) {
            (Ok(Value::I(got)), Some(want)) => prop_assert_eq!(got, want, "{}", text),
            (Err(_), None) => {}
            (got, want) => prop_assert!(false, "{}: got {:?}, want {:?}", text, got, want),
        }
    }

    /// An integer register reads back the value its bytes encode, for every
    /// length, byte order and signedness.
    #[test]
    fn int_registers_decode_their_bytes(
        bytes in prop::collection::vec(any::<u8>(), 8),
        len in prop::sample::select(vec![1usize, 2, 3, 4, 8]),
        big in any::<bool>(),
        signed in any::<bool>(),
    ) {
        let xml = wrap_xml(&format!(
            "<IntReg Name=\"R\"><Address>0x10</Address><Length>{len}</Length><AccessMode>RW</AccessMode>\
             <pPort>Device</pPort><Sign>{}</Sign><Endianess>{}</Endianess></IntReg>",
            if signed { "Signed" } else { "Unsigned" },
            if big { "BigEndian" } else { "LittleEndian" },
        ));
        let mut g = parse_xml(&xml).unwrap();
        let port = MockPort::new(0x40);
        port.mem.lock()[0x10..0x10 + len].copy_from_slice(&bytes[..len]);
        let id = g.lookup("R").unwrap();
        let want = field(register_value(&bytes[..len], big), 0, 8 * len as u32 - 1, signed);
        prop_assert_eq!(g.int_value(id, &port).unwrap(), want);
    }

    /// A masked field reads exactly its bits, honouring GenICam's bit
    /// numbering per byte order, and writing a value it can hold reads back
    /// that value while every other bit of the register is preserved.
    #[test]
    fn masked_fields_read_and_write_exactly_their_bits(
        (len, big, lo, hi) in bit_range(),
        signed in any::<bool>(),
        bytes in prop::collection::vec(any::<u8>(), 8),
        written in any::<i64>(),
    ) {
        let (lsb, msb) = xml_bits(len, big, lo, hi);
        let mut g = parse_xml(&wrap_xml(&masked_reg(len, big, signed, lsb, msb))).unwrap();
        let port = MockPort::new(0x40);
        port.mem.lock()[0x10..0x10 + len].copy_from_slice(&bytes[..len]);
        let id = g.lookup("R").unwrap();
        let before = register_value(&bytes[..len], big);
        prop_assert_eq!(g.int_value(id, &port).unwrap(), field(before, lo, hi, signed));

        let width = hi - lo + 1;
        let value = if width == 64 { written } else { field(written as u64, 0, width - 1, signed) };
        g.set_int_value(id, value, &port).unwrap();
        prop_assert_eq!(g.int_value(id, &port).unwrap(), value);
        let after = register_value(&port.mem.lock()[0x10..0x10 + len], big);
        let mask = if width == 64 { u64::MAX } else { ((1u64 << width) - 1) << lo };
        prop_assert_eq!(after & !mask, before & !mask, "bits outside the field changed");
    }

    /// A little-endian field whose bit range does not fit its register is
    /// an error on access.
    #[test]
    fn out_of_range_little_endian_bits_are_errors(
        len in prop::sample::select(vec![1usize, 2, 4, 8]),
        lsb in 0u32..200,
        msb in 0u32..200,
        signed in any::<bool>(),
    ) {
        prop_assume!(msb < lsb || msb >= 8 * len as u32);
        let mut g = parse_xml(&wrap_xml(&masked_reg(len, false, signed, lsb, msb))).unwrap();
        let port = MockPort::new(0x40);
        let id = g.lookup("R").unwrap();
        prop_assert!(g.int_value(id, &port).is_err());
        prop_assert!(g.set_int_value(id, 1, &port).is_err());
    }

    /// Float registers read the IEEE value their bytes encode, and write
    /// back what they read.
    #[test]
    fn float_registers_decode_their_bytes(
        bytes in prop::collection::vec(any::<u8>(), 8),
        len in prop::sample::select(vec![4usize, 8]),
        big in any::<bool>(),
    ) {
        let xml = wrap_xml(&format!(
            "<FloatReg Name=\"R\"><Address>0x10</Address><Length>{len}</Length><AccessMode>RW</AccessMode>\
             <pPort>Device</pPort><Cachable>NoCache</Cachable><Endianess>{}</Endianess></FloatReg>",
            if big { "BigEndian" } else { "LittleEndian" },
        ));
        let mut g = parse_xml(&xml).unwrap();
        let port = MockPort::new(0x40);
        port.mem.lock()[0x10..0x10 + len].copy_from_slice(&bytes[..len]);
        let id = g.lookup("R").unwrap();
        let raw = register_value(&bytes[..len], big);
        let want = if len == 4 { f64::from(f32::from_bits(raw as u32)) } else { f64::from_bits(raw) };
        let got = g.float_value(id, &port).unwrap();
        prop_assert!(got == want || (got.is_nan() && want.is_nan()), "{} vs {}", got, want);
        if !want.is_nan() {
            g.set_float_value(id, got, &port).unwrap();
            prop_assert_eq!(&port.mem.lock()[0x10..0x10 + len], &bytes[..len]);
        }
    }

    /// A register whose length another register supplies stays usable for
    /// any sane length the device reports.
    #[test]
    fn device_supplied_lengths_in_range_are_honoured(length in 0i64..=4096, signed in any::<bool>()) {
        let mut g = parse_xml(&wrap_xml(&plength_xml(signed))).unwrap();
        let port = MockPort::new(0x2000);
        port.mem.lock()[..8].copy_from_slice(&length.to_le_bytes());
        let id = g.lookup("Blob").unwrap();
        let (_, largest) = largest_allocation(|| { let _ = g.string_value(id, &port); });
        prop_assert!(largest <= 4096 + 1024, "allocated {} bytes", largest);
    }
}

fn plength_xml(signed: bool) -> String {
    format!(
        "<StringReg Name=\"Blob\"><Address>0x100</Address><pLength>BlobLength</pLength>\
         <AccessMode>RW</AccessMode><pPort>Device</pPort><Cachable>NoCache</Cachable></StringReg>\
         <IntReg Name=\"BlobLength\"><Address>0x0</Address><Length>8</Length><AccessMode>RO</AccessMode>\
         <pPort>Device</pPort><Cachable>NoCache</Cachable><Sign>{}</Sign><Endianess>LittleEndian</Endianess></IntReg>",
        if signed { "Signed" } else { "Unsigned" }
    )
}

proptest! {
    #![proptest_config(fuzzing::config(16))]

    /// Every node of a real vendor description evaluates against arbitrary
    /// register contents without panicking.
    #[test]
    fn imperx_evaluates_against_arbitrary_registers(seed in any::<u64>()) {
        let mut g = parse_xml(&vendor_xml("Imperx.xml")).unwrap();
        evaluate_everything(&mut g, &NoisePort(seed));
    }

    /// The same for a description with register lengths that come from
    /// other registers (Hikrobot's `DPCValueAll` is `DPCIndexMax * 4`
    /// bytes): no evaluation panics or allocates more than one sane
    /// transfer, whatever the length registers hold.
    #[test]
    fn hikrobot_evaluates_against_arbitrary_registers(seed in any::<u64>()) {
        let mut g = parse_xml(&vendor_xml("Hikrobot.xml")).unwrap();
        let port = NoisePort(seed);
        let (r, largest) = largest_allocation(|| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| evaluate_everything(&mut g, &port)))
        });
        prop_assert!(r.is_ok(), "evaluation panicked");
        prop_assert!(largest <= ALLOCATION_BOUND, "allocated {} bytes", largest);
    }

    /// Byte-level corruption of a vendor description (overwritten,
    /// dropped or inserted bytes) is either rejected by the parser or
    /// yields a graph that evaluates without panicking.
    #[test]
    fn corrupted_imperx_parses_or_is_rejected(
        edits in prop::collection::vec((any::<prop::sample::Index>(), any::<u8>(), any::<u8>()), 1..8),
        seed in any::<u64>(),
    ) {
        let text = mutate(&vendor_xml("Imperx.xml"), &edits);
        if let Ok(mut g) = parse_xml(&text) {
            evaluate_everything(&mut g, &NoisePort(seed));
        }
    }
}

proptest! {
    #![proptest_config(fuzzing::config(256))]

    /// Any bit flipped in a stored or deflated archive is caught by the
    /// entry's CRC-32: the reader returns an error or the original content,
    /// never different bytes.
    #[test]
    fn a_flipped_zip_bit_never_yields_different_content(
        content in prop::collection::vec(any::<u8>(), 1..2048),
        deflate in any::<bool>(),
        bit in any::<prop::sample::Index>(),
    ) {
        let z = zip("cam.xml", &content, deflate);
        let flipped = fuzzing::flip_bit(&z, bit.index(z.len() * 8));
        if let Ok(out) = unzip(&flipped) {
            prop_assert_eq!(out, content);
        }
    }

    /// A big-endian field whose bit range does not fit its register is an
    /// error on access, as it is for little-endian registers.
    #[test]
    fn out_of_range_big_endian_bits_are_errors(
        len in prop::sample::select(vec![1usize, 2, 4, 8]),
        lsb in 0u32..200,
        msb in 0u32..200,
    ) {
        prop_assume!(lsb >= 8 * len as u32 || msb >= 8 * len as u32);
        let mut g = parse_xml(&wrap_xml(&masked_reg(len, true, false, lsb, msb))).unwrap();
        let port = MockPort::new(0x40);
        let id = g.lookup("R").unwrap();
        let read = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| g.int_value(id, &port)));
        prop_assert!(matches!(read, Ok(Err(_))), "read: {:?}", read.map(|r| r.map_err(|e| e.to_string())));
    }

    /// A negative or absurd length read from the device fails the access
    /// instead of panicking or allocating it.
    #[test]
    fn device_supplied_lengths_out_of_range_are_errors(
        length in prop_oneof![i64::MIN..0, (ALLOCATION_BOUND as i64 + 1)..(1i64 << 31)],
    ) {
        let mut g = parse_xml(&wrap_xml(&plength_xml(true))).unwrap();
        let port = MockPort::new(0x2000);
        port.mem.lock()[..8].copy_from_slice(&length.to_le_bytes());
        let id = g.lookup("Blob").unwrap();
        let (read, largest) = largest_allocation(|| {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| g.string_value(id, &port)))
        });
        prop_assert!(matches!(read, Ok(Err(_))), "length {}: {:?}", length, read.map(|r| r.map_err(|e| e.to_string())));
        prop_assert!(largest <= ALLOCATION_BOUND, "allocated {} bytes", largest);
    }
}

/// Runs this test binary again with only `test` selected and `CHILD` set,
/// so a body that could take the whole process down (a stack overflow
/// aborts) reports as a failed test instead.
fn in_child_process(test: &str, body: impl FnOnce()) {
    if std::env::var_os("TELEGENIC_FUZZ_CHILD").is_some() {
        body();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test, "--include-ignored", "--test-threads=1"])
        .env("TELEGENIC_FUZZ_CHILD", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .expect("rerun the test binary");
    assert!(status.success(), "{test} crashed the process: {status}");
}

/// A self-referential value link — one corrupted pValue name is enough —
/// is reported as circular by every accessor, never followed forever.
#[test]
fn cyclic_value_links_are_errors_for_every_accessor() {
    in_child_process("cyclic_value_links_are_errors_for_every_accessor", || {
        let mut g = parse_xml(&wrap_xml(
            "<Integer Name=\"A\"><pValue>B</pValue></Integer>\
             <Integer Name=\"B\"><pValue>A</pValue></Integer>\
             <String Name=\"S\"><pValue>T</pValue></String>\
             <String Name=\"T\"><pValue>S</pValue></String>",
        ))
        .unwrap();
        let port = MockPort::new(0x10);
        let a = g.lookup("A").unwrap();
        assert!(g.int_value(a, &port).is_err());
        let _ = g.access_mode(a);
        let s = g.lookup("S").unwrap();
        assert!(g.string_value(s, &port).is_err());
        assert!(g.set_string_value(s, "x", &port).is_err());
    });
}
