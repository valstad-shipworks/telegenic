//! Property tests for the GVSP decoders: the packet header in both id modes,
//! the image leader, and the device-tick to nanosecond conversion applied to
//! leader timestamps. Frame reassembly is covered next to the receiver, in
//! `gige::stream::runner`.

mod fuzzing;

use proptest::prelude::*;
use telegenic::gige::proto::gvcp::GvcpStatus;
use telegenic::gige::proto::gvsp::{
    self, ContentType, GvspView, ImageLeader, PAYLOAD_TYPE_CHUNK_EXTENSION, PixelFormat,
    timestamp_to_ns,
};

fn content_bits(c: ContentType) -> u8 {
    match c {
        ContentType::Leader => 1,
        ContentType::Trailer => 2,
        ContentType::Payload => 3,
        ContentType::AllIn => 4,
        ContentType::H264 => 5,
        ContentType::Multizone => 6,
        ContentType::Multipart => 7,
        ContentType::GenDc => 8,
        ContentType::Unknown(b) => b,
    }
}

fn header(status: u16, extended: bool, frame_id: u64, packet_id: u32, content: u8) -> Vec<u8> {
    let mut b = status.to_be_bytes().to_vec();
    if extended {
        b.extend_from_slice(&0u16.to_be_bytes());
        b.extend_from_slice(&(0x8000_0000 | (u32::from(content) << 24)).to_be_bytes());
        b.extend_from_slice(&frame_id.to_be_bytes());
        b.extend_from_slice(&packet_id.to_be_bytes());
    } else {
        b.extend_from_slice(&(frame_id as u16).to_be_bytes());
        b.extend_from_slice(
            &((u32::from(content) << 24) | (packet_id & 0x00ff_ffff)).to_be_bytes(),
        );
    }
    b
}

#[derive(Debug, Clone)]
struct Leader {
    payload_type: u16,
    chunks_flag: bool,
    timestamp: u64,
    pixel_format: u32,
    width: u32,
    height: u32,
    x_offset: u32,
    y_offset: u32,
    x_padding: u16,
    y_padding: u16,
}

fn leader() -> impl Strategy<Value = Leader> {
    (
        (0u16..0x4000, any::<bool>(), any::<u64>(), any::<u32>()),
        (any::<u32>(), any::<u32>(), any::<u32>(), any::<u32>()),
        (any::<u16>(), any::<u16>()),
    )
        .prop_map(
            |((payload_type, chunks_flag, timestamp, pf), (w, h, x, y), (xp, yp))| Leader {
                payload_type,
                chunks_flag,
                timestamp,
                pixel_format: pf,
                width: w,
                height: h,
                x_offset: x,
                y_offset: y,
                x_padding: xp,
                y_padding: yp,
            },
        )
}

fn leader_bytes(l: &Leader) -> Vec<u8> {
    let raw_type = l.payload_type
        | if l.chunks_flag {
            PAYLOAD_TYPE_CHUNK_EXTENSION
        } else {
            0
        };
    let mut b = 0u16.to_be_bytes().to_vec();
    b.extend_from_slice(&raw_type.to_be_bytes());
    b.extend_from_slice(&l.timestamp.to_be_bytes());
    for v in [l.pixel_format, l.width, l.height, l.x_offset, l.y_offset] {
        b.extend_from_slice(&v.to_be_bytes());
    }
    b.extend_from_slice(&l.x_padding.to_be_bytes());
    b.extend_from_slice(&l.y_padding.to_be_bytes());
    b
}

fn exact_ns(ticks: u64, frequency: u64) -> u128 {
    u128::from(ticks) * 1_000_000_000 / u128::from(frequency)
}

proptest! {
    #![proptest_config(fuzzing::config(1024))]

    /// Whatever arrives on the stream socket decodes without panicking; a
    /// decoded packet's data is the datagram's tail after its header.
    #[test]
    fn arbitrary_datagrams_decode_without_panicking(
        bytes in prop::collection::vec(any::<u8>(), 0..600),
    ) {
        if let Some(v) = GvspView::parse(&bytes) {
            let header = if v.extended_ids { 20 } else { 8 };
            prop_assert_eq!(v.data, &bytes[header..]);
            prop_assert_eq!(v.extended_ids, bytes[4] & 0x80 != 0);
            if !v.extended_ids {
                prop_assert!(v.packet_id <= gvsp::PACKET_ID_MASK);
                prop_assert!(v.frame_id <= u64::from(u16::MAX));
            }
            if v.content_type == ContentType::Leader {
                let _ = ImageLeader::parse(v.data);
            }
        }
        let _ = ImageLeader::parse(&bytes);
    }

    /// Every header field survives encode → decode in both id modes.
    #[test]
    fn headers_roundtrip(
        status in any::<u16>(),
        extended in any::<bool>(),
        frame_id in any::<u64>(),
        packet_id in any::<u32>(),
        content in 0u8..0x80,
        data in prop::collection::vec(any::<u8>(), 0..64),
    ) {
        let mut bytes = header(status, extended, frame_id, packet_id, content);
        bytes.extend_from_slice(&data);
        let v = GvspView::parse(&bytes).expect("a well-formed packet decodes");
        prop_assert_eq!(v.status, GvcpStatus(status));
        prop_assert_eq!(v.extended_ids, extended);
        prop_assert_eq!(content_bits(v.content_type), content);
        if extended {
            prop_assert_eq!((v.frame_id, v.packet_id), (frame_id, packet_id));
        } else {
            prop_assert_eq!(
                (v.frame_id, v.packet_id),
                (u64::from(frame_id as u16), packet_id & gvsp::PACKET_ID_MASK)
            );
        }
        prop_assert_eq!(v.data, &data[..]);
    }

    /// An extended-id packet cut inside its 20-byte header is rejected
    /// rather than decoded with a short frame or packet id.
    #[test]
    fn truncated_extended_headers_are_rejected(
        frame_id in any::<u64>(),
        packet_id in any::<u32>(),
        keep in 0usize..20,
    ) {
        let bytes = header(0, true, frame_id, packet_id, 3);
        prop_assert!(GvspView::parse(&bytes[..keep]).is_none());
    }

    /// Every leader field survives encode → decode; trailing bytes past the
    /// image leader are ignored, and a generic 12-byte leader reads its
    /// image fields as zero.
    #[test]
    fn image_leaders_roundtrip(l in leader(), trailing in prop::collection::vec(any::<u8>(), 0..16)) {
        let mut bytes = leader_bytes(&l);
        bytes.extend_from_slice(&trailing);
        let d = ImageLeader::parse(&bytes).expect("a full leader decodes");
        prop_assert_eq!(d.payload_type, l.payload_type);
        prop_assert_eq!(
            d.has_chunks,
            l.chunks_flag
                || l.payload_type == gvsp::PAYLOAD_TYPE_CHUNK_DATA
                || l.payload_type == gvsp::PAYLOAD_TYPE_EXTENDED_CHUNK_DATA
        );
        prop_assert_eq!(d.timestamp_ticks, l.timestamp);
        prop_assert_eq!(d.pixel_format, PixelFormat(l.pixel_format));
        prop_assert_eq!(
            (d.width, d.height, d.x_offset, d.y_offset, d.x_padding, d.y_padding),
            (l.width, l.height, l.x_offset, l.y_offset, l.x_padding, l.y_padding)
        );

        let generic = ImageLeader::parse(&bytes[..12]).expect("a generic leader decodes");
        prop_assert_eq!((generic.timestamp_ticks, generic.width, generic.height), (l.timestamp, 0, 0));
        prop_assert!(ImageLeader::parse(&bytes[..11]).is_none());
    }

    /// Tick conversion is exact (floor of `ticks * 1e9 / frequency`) for any
    /// tick frequency a device reports and any 64-bit timestamp whose time
    /// fits in u64 nanoseconds, in the realistic band up to 1 GHz.
    #[test]
    fn timestamps_convert_exactly_at_realistic_tick_frequencies(
        frequency in 1u64..=1_000_000_000,
        ticks in any::<u64>(),
    ) {
        let exact = exact_ns(ticks, frequency);
        prop_assume!(exact <= u128::from(u64::MAX));
        prop_assert_eq!(u128::from(timestamp_to_ns(ticks, frequency)), exact);
    }

    /// A device timestamp counter narrower than 64 bits (or one that wraps
    /// at any modulus) converts each raw value on its own: nothing assumes a
    /// width, so the converted time is exact and steps back by exactly one
    /// period's worth of nanoseconds at the wrap.
    #[test]
    fn a_wrapping_device_counter_converts_each_value_exactly(
        frequency in 1u64..=1_000_000_000,
        modulus in 2u64..=u64::from(u32::MAX) * 64,
        start in any::<u64>(),
        step in 1u64..1_000_000,
    ) {
        let before = start % modulus;
        let after = (before + step % modulus) % modulus;
        for t in [before, after] {
            prop_assert_eq!(u128::from(timestamp_to_ns(t, frequency)), exact_ns(t, frequency));
        }
    }
}

proptest! {
    #![proptest_config(fuzzing::config(512))]

    /// Conversion never panics for any tick frequency register value and any
    /// leader timestamp, and is exact whenever the time fits in u64
    /// nanoseconds.
    #[test]
    fn timestamps_convert_at_any_tick_frequency(frequency in 1u64.., ticks in any::<u64>()) {
        let got = std::panic::catch_unwind(|| timestamp_to_ns(ticks, frequency));
        prop_assert!(got.is_ok(), "panicked for {} ticks at {} Hz", ticks, frequency);
        let exact = exact_ns(ticks, frequency);
        if exact <= u128::from(u64::MAX) {
            prop_assert_eq!(u128::from(got.unwrap()), exact);
        }
    }

    /// The image-size helper accepts any geometry a leader can carry.
    #[test]
    fn image_size_never_panics_on_decoded_geometry(l in leader()) {
        let d = ImageLeader::parse(&leader_bytes(&l)).unwrap();
        let got = std::panic::catch_unwind(|| d.pixel_format.image_size(d.width, d.height));
        prop_assert!(got.is_ok(), "panicked for {}x{} {}", d.width, d.height, d.pixel_format);
    }
}

#[test]
fn a_zero_tick_frequency_converts_to_zero() {
    assert_eq!(timestamp_to_ns(u64::MAX, 0), 0);
}
