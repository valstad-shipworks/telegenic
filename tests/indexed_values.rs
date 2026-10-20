//! `pIndex` values, shaped like a Basler dart's `ExposureTime`: a `Float`
//! reaching an `Integer` that picks one of its `pValueIndexed` /
//! `ValueIndexed` entries by its index node's value, or its `pValueDefault`.

use telegenic::genicam::port::MockPort;
use telegenic::genicam::{AccessMode, parse_xml};

const SELECTED_REG: u64 = 0x10;
const DEFAULT_REG: u64 = 0x20;

fn int_reg(name: &str, address: u64) -> String {
    format!(
        "<IntReg Name=\"{name}\"><Address>{address:#x}</Address><Length>4</Length>\
         <AccessMode>RW</AccessMode><pPort>Device</pPort><Sign>Unsigned</Sign>\
         <Endianess>BigEndian</Endianess></IntReg>"
    )
}

fn camera() -> String {
    let body = format!(
        "<Float Name=\"ExposureTime\"><pValue>Exposure</pValue></Float>\
         <Float Name=\"Exposure\"><pValue>ExposureRaw</pValue></Float>\
         <Integer Name=\"ExposureRaw\">\
           <pIndex>ExposureSelector</pIndex>\
           <pValueIndexed Index=\"1\">Selected</pValueIndexed>\
           <ValueIndexed Index=\"2\">7</ValueIndexed>\
           <pValueDefault>Default</pValueDefault>\
         </Integer>\
         <Integer Name=\"ExposureSelector\"><Value>0</Value></Integer>\
         <Integer Name=\"Selected\"><pValue>SelectedReg</pValue></Integer>\
         <Integer Name=\"Default\"><pValue>DefaultReg</pValue></Integer>\
         {}{}",
        int_reg("SelectedReg", SELECTED_REG),
        int_reg("DefaultReg", DEFAULT_REG),
    );
    format!(
        r#"<?xml version="1.0"?><RegisterDescription ModelName="T" VendorName="T">{body}<Port Name="Device"/></RegisterDescription>"#
    )
}

#[test]
fn an_index_with_no_matching_entry_reads_and_writes_the_default() {
    let mut g = parse_xml(&camera()).unwrap();
    let port = MockPort::new(0x40);
    let exposure = g.lookup("ExposureTime").unwrap();

    assert_eq!(g.access_mode(exposure), AccessMode::RW);
    g.set_float_value(exposure, 500.0, &port).unwrap();
    assert_eq!(port.u32_be(DEFAULT_REG), 500);
    assert_eq!(port.u32_be(SELECTED_REG), 0, "the other entry is untouched");
    assert_eq!(g.float_value(exposure, &port).unwrap(), 500.0);
}

#[test]
fn the_index_picks_the_entry_at_read_and_write_time() {
    let mut g = parse_xml(&camera()).unwrap();
    let port = MockPort::new(0x40);
    let exposure = g.lookup("ExposureTime").unwrap();
    let selector = g.lookup("ExposureSelector").unwrap();

    g.set_int_value(selector, 1, &port).unwrap();
    g.set_float_value(exposure, 1200.0, &port).unwrap();
    assert_eq!(port.u32_be(SELECTED_REG), 1200);
    assert_eq!(port.u32_be(DEFAULT_REG), 0);
    assert_eq!(g.float_value(exposure, &port).unwrap(), 1200.0);

    g.set_int_value(selector, 2, &port).unwrap();
    assert_eq!(g.float_value(exposure, &port).unwrap(), 7.0);
    assert!(
        g.set_float_value(exposure, 9.0, &port).is_err(),
        "a literal entry is not writable"
    );
}
