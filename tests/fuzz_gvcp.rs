//! Property tests for the GVCP decoders: acknowledges, commands (the
//! device-initiated events and everything the driver encodes), the
//! discovery reply's device description block, and the request-id sequence.
//!
//! GVCP has no checksum of its own; the UDP checksum is its only guard
//! against flipped payload bits. What the decoder can decide is framing: the
//! header's length field against the datagram, so a truncated datagram must
//! be rejected and trailing bytes past the length must never leak into the
//! payload.

mod fuzzing;

use std::net::Ipv4Addr;

use proptest::prelude::*;
use telegenic::gige::DeviceInfo;
use telegenic::gige::proto::bootstrap;
use telegenic::gige::proto::gvcp::{self, Ack, Cmd, GvcpStatus};

fn ack_bytes(status: u16, answer: u16, ack_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::with_capacity(8 + payload.len());
    b.extend_from_slice(&status.to_be_bytes());
    b.extend_from_slice(&answer.to_be_bytes());
    b.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    b.extend_from_slice(&ack_id.to_be_bytes());
    b.extend_from_slice(payload);
    b
}

fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_be_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}

proptest! {
    #![proptest_config(fuzzing::config(1024))]

    /// Whatever arrives on the control socket decodes without panicking,
    /// and a decoded acknowledge's payload is exactly the bytes its length
    /// field covers.
    #[test]
    fn arbitrary_datagrams_decode_without_panicking(
        bytes in prop::collection::vec(any::<u8>(), 0..1200),
    ) {
        if let Some(ack) = Ack::parse(&bytes) {
            let length = usize::from(u16::from_be_bytes([bytes[4], bytes[5]]));
            prop_assert_eq!(ack.payload, &bytes[8..8 + length]);
            prop_assert_eq!(ack.register_values().count(), length / 4);
            let _ = ack.pending_ack_timeout_ms();
        } else {
            prop_assert!(
                bytes.len() < 8
                    || bytes.len() - 8 < usize::from(u16::from_be_bytes([bytes[4], bytes[5]]))
            );
        }
        if let Some(cmd) = Cmd::parse(&bytes) {
            prop_assert!(gvcp::is_cmd(&bytes));
            let length = usize::from(u16::from_be_bytes([bytes[4], bytes[5]]));
            prop_assert_eq!(cmd.payload, &bytes[8..8 + length]);
        }
        let _ = DeviceInfo::parse(&bytes);
    }

    /// Every field of an acknowledge survives encode → decode.
    #[test]
    fn acks_roundtrip(
        status in any::<u16>(),
        answer in any::<u16>(),
        ack_id in any::<u16>(),
        payload in prop::collection::vec(any::<u8>(), 0..600),
        trailing in prop::collection::vec(any::<u8>(), 0..32),
    ) {
        let mut bytes = ack_bytes(status, answer, ack_id, &payload);
        bytes.extend_from_slice(&trailing);
        let ack = Ack::parse(&bytes).expect("a well-formed ack decodes");
        prop_assert_eq!(ack.status, GvcpStatus(status));
        prop_assert_eq!(ack.answer, answer);
        prop_assert_eq!(ack.ack_id, ack_id);
        prop_assert_eq!(ack.payload, &payload[..]);
        let values: Vec<u32> = ack.register_values().collect();
        let expected: Vec<u32> = payload
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| u32::from_be_bytes(*c))
            .collect();
        prop_assert_eq!(values, expected);
    }

    /// An acknowledge cut short anywhere inside its declared length is
    /// rejected, never decoded with a shorter payload.
    #[test]
    fn truncated_acks_are_rejected(
        payload in prop::collection::vec(any::<u8>(), 1..600),
        cut in any::<prop::sample::Index>(),
    ) {
        let bytes = ack_bytes(0, gvcp::READ_MEMORY_ACK, 7, &payload);
        let keep = cut.index(bytes.len());
        prop_assert!(Ack::parse(&bytes[..keep]).is_none());
    }

    /// One flipped bit changes exactly the field it lands in: the decoder
    /// either rejects the datagram (a length now past its end) or reports
    /// every other field unchanged. A flipped length bit that still fits
    /// yields a payload that is a prefix of the datagram's bytes.
    #[test]
    fn a_flipped_ack_bit_changes_only_its_field(
        payload in prop::collection::vec(any::<u8>(), 0..64),
        trailing in prop::collection::vec(any::<u8>(), 0..64),
        bit in any::<prop::sample::Index>(),
    ) {
        let mut bytes = ack_bytes(0, gvcp::READ_REGISTER_ACK, 0x1234, &payload);
        bytes.extend_from_slice(&trailing);
        let bit = bit.index(bytes.len() * 8);
        let flipped = fuzzing::flip_bit(&bytes, bit);
        let byte = bit / 8;
        match Ack::parse(&flipped) {
            None => prop_assert!((4..6).contains(&byte), "rejected a flip at byte {}", byte),
            Some(ack) => {
                prop_assert_eq!(ack.status.0 != 0, byte < 2);
                prop_assert_eq!(ack.answer != gvcp::READ_REGISTER_ACK, (2..4).contains(&byte));
                prop_assert_eq!(ack.ack_id != 0x1234, (6..8).contains(&byte));
                if (4..6).contains(&byte) {
                    prop_assert_eq!(ack.payload, &flipped[8..8 + ack.payload.len()]);
                } else if byte >= 8 + payload.len() {
                    prop_assert_eq!(ack.payload, &payload[..]);
                } else if byte >= 8 {
                    let diff: Vec<usize> = (0..payload.len())
                        .filter(|&i| ack.payload[i] != payload[i])
                        .collect();
                    prop_assert_eq!(diff, vec![byte - 8]);
                }
            }
        }
    }

    /// Commands the driver encodes decode back to the same command, flags,
    /// request id and operands.
    #[test]
    fn encoded_register_commands_roundtrip(
        addrs in prop::collection::vec(any::<u32>(), 0..135),
        values in prop::collection::vec(any::<u32>(), 0..67),
        id in any::<u16>(),
    ) {
        let bytes = gvcp::encode_read_reg(&addrs, id);
        let cmd = Cmd::parse(&bytes).expect("read reg decodes");
        prop_assert_eq!(
            (cmd.command, cmd.flags, cmd.req_id),
            (gvcp::READ_REGISTER_CMD, gvcp::FLAG_ACK_REQUIRED, id)
        );
        let decoded: Vec<u32> = (0..addrs.len()).map(|i| u32_at(cmd.payload, 4 * i)).collect();
        prop_assert_eq!(&decoded, &addrs);
        prop_assert_eq!(cmd.payload.len(), 4 * addrs.len());

        let pairs: Vec<(u32, u32)> = addrs.iter().copied().zip(values.iter().copied()).collect();
        let bytes = gvcp::encode_write_reg(&pairs, id);
        let cmd = Cmd::parse(&bytes).expect("write reg decodes");
        prop_assert_eq!((cmd.command, cmd.req_id), (gvcp::WRITE_REGISTER_CMD, id));
        let decoded: Vec<(u32, u32)> = (0..pairs.len())
            .map(|i| (u32_at(cmd.payload, 8 * i), u32_at(cmd.payload, 8 * i + 4)))
            .collect();
        prop_assert_eq!(decoded, pairs);
    }

    #[test]
    fn encoded_memory_commands_roundtrip(
        addr in any::<u32>(),
        count in any::<u16>(),
        words in prop::collection::vec(any::<u32>(), 0..=gvcp::DATA_SIZE_MAX / 4),
        id in any::<u16>(),
    ) {
        let bytes = gvcp::encode_read_mem(addr, count, id);
        let cmd = Cmd::parse(&bytes).expect("read mem decodes");
        prop_assert_eq!((cmd.command, cmd.req_id), (gvcp::READ_MEMORY_CMD, id));
        prop_assert_eq!((u32_at(cmd.payload, 0), u32_at(cmd.payload, 4)), (addr, u32::from(count)));

        let data: Vec<u8> = words.iter().flat_map(|w| w.to_be_bytes()).collect();
        let bytes = gvcp::encode_write_mem(addr, &data, id);
        let cmd = Cmd::parse(&bytes).expect("write mem decodes");
        prop_assert_eq!((cmd.command, cmd.req_id), (gvcp::WRITE_MEMORY_CMD, id));
        prop_assert_eq!(u32_at(cmd.payload, 0), addr);
        prop_assert_eq!(&cmd.payload[4..], &data[..]);
    }

    /// Resend requests carry the block and packet range in the layout the
    /// id mode prescribes; standard ids keep only 24 packet-id bits.
    #[test]
    fn encoded_resend_requests_roundtrip(
        frame_id in any::<u64>(),
        first in any::<u32>(),
        last in any::<u32>(),
        extended in any::<bool>(),
        id in any::<u16>(),
    ) {
        let mut buf = [0u8; gvcp::RESEND_MAX_LEN];
        let n = gvcp::encode_packet_resend(&mut buf, frame_id, first, last, extended, id);
        let cmd = Cmd::parse(&buf[..n]).expect("resend decodes");
        prop_assert_eq!((cmd.command, cmd.req_id), (gvcp::PACKET_RESEND_CMD, id));
        prop_assert_eq!(cmd.flags & gvcp::FLAG_ACK_REQUIRED, 0, "resends are never acknowledged");
        if extended {
            prop_assert_eq!(cmd.flags, gvcp::FLAG_EXTENDED_IDS);
            prop_assert_eq!(
                (u32_at(cmd.payload, 4), u32_at(cmd.payload, 8)),
                (first, last)
            );
            let id = u64::from_be_bytes(cmd.payload[12..20].try_into().unwrap());
            prop_assert_eq!(id, frame_id);
        } else {
            prop_assert_eq!(u32_at(cmd.payload, 0), frame_id as u32);
            prop_assert_eq!(
                (u32_at(cmd.payload, 4), u32_at(cmd.payload, 8)),
                (first & 0x00ff_ffff, last & 0x00ff_ffff)
            );
        }
    }

    #[test]
    fn encoded_force_ip_and_event_acks_roundtrip(
        mac in any::<[u8; 6]>(),
        ip in any::<u32>(),
        mask in any::<u32>(),
        gateway in any::<u32>(),
        event in prop::sample::select(vec![gvcp::EVENT_CMD, gvcp::EVENTDATA_CMD]),
        id in any::<u16>(),
    ) {
        let bytes = gvcp::encode_force_ip(
            mac,
            Ipv4Addr::from(ip),
            Ipv4Addr::from(mask),
            Ipv4Addr::from(gateway),
            id,
        );
        let cmd = Cmd::parse(&bytes).expect("force ip decodes");
        prop_assert_eq!((cmd.command, cmd.req_id, cmd.payload.len()), (gvcp::FORCEIP_CMD, id, 56));
        prop_assert_eq!(&cmd.payload[2..8], &mac[..]);
        prop_assert_eq!(
            (u32_at(cmd.payload, 20), u32_at(cmd.payload, 36), u32_at(cmd.payload, 52)),
            (ip, mask, gateway)
        );

        let ack = gvcp::encode_event_ack(event, id);
        let ack = Ack::parse(&ack).expect("event ack decodes");
        prop_assert_eq!((ack.status, ack.answer, ack.ack_id), (GvcpStatus::SUCCESS, event + 1, id));
        prop_assert!(!gvcp::is_cmd(&gvcp::encode_event_ack(event, id)));
    }

    /// A device description block decodes field by field, through the
    /// discovery acknowledge that carries it.
    #[test]
    fn discovery_replies_roundtrip(
        version in any::<u32>(),
        mode in any::<u32>(),
        mac in any::<[u8; 6]>(),
        ip_config in any::<(u32, u32)>(),
        addrs in any::<(u32, u32, u32)>(),
        manufacturer in "[ -~]{0,31}",
        model in "[ -~]{0,31}",
        version_str in "[ -~]{0,31}",
        info in "[ -~]{0,47}",
        serial in "[ -~]{0,15}",
        user_name in "[ -~]{0,15}",
        extra in prop::collection::vec(any::<u8>(), 0..16),
    ) {
        let mut block = vec![0u8; bootstrap::DISCOVERY_DATA_SIZE];
        let mut put = |at: u32, bytes: &[u8]| {
            block[at as usize..at as usize + bytes.len()].copy_from_slice(bytes);
        };
        put(bootstrap::VERSION, &version.to_be_bytes());
        put(bootstrap::DEVICE_MODE, &mode.to_be_bytes());
        put(0x0a, &mac);
        put(bootstrap::SUPPORTED_IP_CONFIG, &ip_config.0.to_be_bytes());
        put(bootstrap::CURRENT_IP_CONFIG, &ip_config.1.to_be_bytes());
        put(bootstrap::CURRENT_IP_ADDRESS, &addrs.0.to_be_bytes());
        put(bootstrap::CURRENT_SUBNET_MASK, &addrs.1.to_be_bytes());
        put(bootstrap::CURRENT_GATEWAY, &addrs.2.to_be_bytes());
        put(bootstrap::MANUFACTURER_NAME, manufacturer.as_bytes());
        put(bootstrap::MODEL_NAME, model.as_bytes());
        put(bootstrap::DEVICE_VERSION, version_str.as_bytes());
        put(bootstrap::MANUFACTURER_INFO, info.as_bytes());
        put(bootstrap::SERIAL_NUMBER, serial.as_bytes());
        put(bootstrap::USER_DEFINED_NAME, user_name.as_bytes());
        block.extend_from_slice(&extra);

        let datagram = ack_bytes(0, gvcp::DISCOVERY_ACK, gvcp::DISCOVERY_ID, &block);
        let ack = Ack::parse(&datagram).expect("discovery ack decodes");
        prop_assert_eq!(ack.answer, gvcp::DISCOVERY_ACK);
        let d = DeviceInfo::parse(ack.payload).expect("block decodes");
        prop_assert_eq!(d.spec_version, ((version >> 16) as u16, version as u16));
        prop_assert_eq!((d.device_mode, d.mac), (mode, mac));
        prop_assert_eq!((d.supported_ip_config, d.current_ip_config), ip_config);
        prop_assert_eq!(
            (d.ip, d.subnet_mask, d.gateway),
            (Ipv4Addr::from(addrs.0), Ipv4Addr::from(addrs.1), Ipv4Addr::from(addrs.2))
        );
        prop_assert_eq!(
            [d.manufacturer, d.model, d.device_version, d.manufacturer_info, d.serial, d.user_defined_name],
            [manufacturer, model, version_str, info, serial, user_name]
        );
        prop_assert!(DeviceInfo::parse(&ack.payload[..bootstrap::DISCOVERY_DATA_SIZE - 1]).is_none());
    }
}

proptest! {
    #![proptest_config(fuzzing::config(32))]

    /// Request ids run 1..=0xfffe and wrap back to 1, from any starting
    /// point (including the reserved 0 and the discovery id): never 0 or
    /// 0xffff, and the k-th id after the first is exactly `first + k`
    /// modulo the 0xfffe usable ids, across as many wraps as it takes.
    #[test]
    fn request_ids_wrap_skipping_zero_and_discovery(start in any::<u16>(), steps in 1u32..300_000) {
        let first = gvcp::next_id(start);
        prop_assert_eq!(first, if start >= 0xfffe { 1 } else { start + 1 });
        let mut id = first;
        for k in 1..steps {
            id = gvcp::next_id(id);
            let expected = ((u32::from(first) - 1 + k) % 0xfffe) as u16 + 1;
            prop_assert_eq!(id, expected, "step {} from {}", k, start);
        }
    }
}

proptest! {
    #![proptest_config(fuzzing::config(256))]

    /// PENDING_ACK's payload is a reserved half-word followed by the 16-bit
    /// time to completion in milliseconds; the reserved half is ignored, so
    /// no PENDING_ACK can stretch a deadline past 65.535 s.
    #[test]
    fn pending_ack_timeout_reads_only_the_time_to_completion(
        reserved in any::<u16>(),
        timeout in any::<u16>(),
    ) {
        let mut payload = reserved.to_be_bytes().to_vec();
        payload.extend_from_slice(&timeout.to_be_bytes());
        let bytes = ack_bytes(0, gvcp::PENDING_ACK, 3, &payload);
        let ack = Ack::parse(&bytes).unwrap();
        prop_assert_eq!(ack.pending_ack_timeout_ms(), Some(u32::from(timeout)));
    }
}

#[cfg(feature = "emulator")]
proptest! {
    #![proptest_config(fuzzing::config(1024))]

    /// The emulated device decodes whatever reaches its GVCP port without
    /// panicking, and answers a command it executed under that command's
    /// request id.
    #[test]
    fn the_emulator_survives_arbitrary_commands(
        bytes in prop::collection::vec(any::<u8>(), 0..600),
        command in prop::sample::select(vec![
            gvcp::READ_REGISTER_CMD,
            gvcp::WRITE_REGISTER_CMD,
            gvcp::READ_MEMORY_CMD,
            gvcp::WRITE_MEMORY_CMD,
            gvcp::PACKET_RESEND_CMD,
            gvcp::DISCOVERY_CMD,
        ]),
        as_cmd in any::<bool>(),
    ) {
        use telegenic::emulator::{DeviceConfig, GigeDevice};
        let mut dev = GigeDevice::new(Ipv4Addr::new(10, 0, 0, 5), &DeviceConfig::default());
        let mut datagram = bytes;
        if as_cmd && datagram.len() >= 8 {
            datagram[0] = gvcp::PACKET_TYPE_CMD;
            datagram[2..4].copy_from_slice(&command.to_be_bytes());
            let len = (datagram.len() - 8) as u16;
            datagram[4..6].copy_from_slice(&len.to_be_bytes());
        }
        let src = "10.0.0.1:40000".parse().unwrap();
        let reaction = dev.handle_datagram(&datagram, src);
        if let (Some(reply), Some(cmd)) = (&reaction.reply, Cmd::parse(&datagram)) {
            let ack = Ack::parse(reply).expect("the emulator's reply decodes");
            let expected_id = if cmd.command == gvcp::DISCOVERY_CMD {
                gvcp::DISCOVERY_ID
            } else {
                cmd.req_id
            };
            prop_assert_eq!(ack.ack_id, expected_id);
            prop_assert_eq!(ack.answer, cmd.command + 1);
        }
    }
}
