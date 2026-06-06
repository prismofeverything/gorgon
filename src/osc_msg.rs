/// Helpers for building and interpreting the OSC messages gorgon uses.
///
/// Address convention:
///   /cv/<channel>   f32  — continuous control value, conventionally 0.0 – 1.0
///   /gate/<channel> i32  — 0 = off, 1 = on
///   /note/<channel> i32  — MIDI note number (0-127)
///   /raw            any  — pass arbitrary OSC through unchanged
use rosc::{OscMessage, OscPacket, OscType};

use crate::config::Remote;

/// A group member's self-description, learned from its periodic advertisement.
/// The named ports are listed in ordinal order — ordinal `i` is the device's
/// channel `i`. The originating IP comes from the receiving socket, not the
/// message, so a spoofed body can't impersonate another member's audio.
#[derive(Debug, Clone, PartialEq)]
pub struct Advertisement {
    pub group: String,
    pub node_id: u128,
    pub device_name: String,
    pub outputs: Vec<String>,
    pub inputs: Vec<String>,
}

/// Build a member advertisement: `/gorgon/adv/<group>` carrying the member's
/// stable id, device name, and its exposed output/input signal names in order.
pub fn advertisement(group: &str, node_id: u128, device_name: &str, remote: &Remote) -> OscPacket {
    let mut args = Vec::with_capacity(4 + remote.outputs.len() + remote.inputs.len());
    args.push(OscType::String(format!("{node_id:032x}")));
    args.push(OscType::String(device_name.to_string()));
    args.push(OscType::Int(remote.outputs.len() as i32));
    args.push(OscType::Int(remote.inputs.len() as i32));
    args.extend(remote.outputs.iter().map(|p| OscType::String(p.name.clone())));
    args.extend(remote.inputs.iter().map(|p| OscType::String(p.name.clone())));
    OscPacket::Message(OscMessage {
        addr: format!("/gorgon/adv/{group}"),
        args,
    })
}

/// Parse a `/gorgon/adv/<group>` advertisement. Returns `None` for any other
/// address or a malformed body, so it's safe to run on every OSC packet.
pub fn parse_advertisement(packet: &OscPacket) -> Option<Advertisement> {
    let OscPacket::Message(m) = packet else { return None };
    let group = m.addr.strip_prefix("/gorgon/adv/")?;
    if group.is_empty() {
        return None;
    }
    let mut args = m.args.iter();
    let node_id = match args.next()? {
        OscType::String(s) => u128::from_str_radix(s, 16).ok()?,
        _ => return None,
    };
    let device_name = match args.next()? {
        OscType::String(s) => s.clone(),
        _ => return None,
    };
    let n_out = match args.next()? {
        OscType::Int(n) if *n >= 0 => *n as usize,
        _ => return None,
    };
    let n_in = match args.next()? {
        OscType::Int(n) if *n >= 0 => *n as usize,
        _ => return None,
    };
    let take_names = |args: &mut std::slice::Iter<'_, OscType>, n: usize| -> Option<Vec<String>> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            match args.next()? {
                OscType::String(s) => v.push(s.clone()),
                _ => return None,
            }
        }
        Some(v)
    };
    let outputs = take_names(&mut args, n_out)?;
    let inputs = take_names(&mut args, n_in)?;
    Some(Advertisement {
        group: group.to_string(),
        node_id,
        device_name,
        outputs,
        inputs,
    })
}

pub fn cv(channel: &str, value: f32) -> OscPacket {
    OscPacket::Message(OscMessage {
        addr: format!("/cv/{channel}"),
        args: vec![OscType::Float(value)],
    })
}

pub fn gate(channel: &str, on: bool) -> OscPacket {
    OscPacket::Message(OscMessage {
        addr: format!("/gate/{channel}"),
        args: vec![OscType::Int(if on { 1 } else { 0 })],
    })
}

pub fn note(channel: &str, midi_note: i32) -> OscPacket {
    OscPacket::Message(OscMessage {
        addr: format!("/note/{channel}"),
        args: vec![OscType::Int(midi_note)],
    })
}

/// Describe an incoming packet in a human-readable way for logging.
pub fn describe(packet: &OscPacket) -> String {
    match packet {
        OscPacket::Message(m) => {
            let args: Vec<String> = m.args.iter().map(fmt_arg).collect();
            format!("{} [{}]", m.addr, args.join(", "))
        }
        OscPacket::Bundle(b) => {
            let msgs: Vec<String> = b.content.iter().map(describe).collect();
            format!("bundle({})", msgs.join(" | "))
        }
    }
}

fn fmt_arg(a: &OscType) -> String {
    match a {
        OscType::Float(v) => format!("{v:.4}"),
        OscType::Double(v) => format!("{v:.4}"),
        OscType::Int(v) => format!("{v}"),
        OscType::Long(v) => format!("{v}"),
        OscType::String(v) => format!("\"{v}\""),
        OscType::Bool(v) => format!("{v}"),
        _ => "?".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ExposedPort, Remote};

    #[test]
    fn advertisement_round_trips() {
        let remote = Remote {
            outputs: vec![
                ExposedPort { name: "kick".into(), channel: 0 },
                ExposedPort { name: "bass".into(), channel: 1 },
            ],
            inputs: vec![ExposedPort { name: "return_l".into(), channel: 8 }],
        };
        let pkt = advertisement("modular-jam", 0xDEAD_BEEF, "ryan-es9", &remote);
        let ad = parse_advertisement(&pkt).expect("advertisement should parse");
        assert_eq!(ad.group, "modular-jam");
        assert_eq!(ad.node_id, 0xDEAD_BEEF);
        assert_eq!(ad.device_name, "ryan-es9");
        assert_eq!(ad.outputs, vec!["kick", "bass"]);
        assert_eq!(ad.inputs, vec!["return_l"]);
    }

    #[test]
    fn ignores_non_advertisement() {
        // Other OSC messages (and other addresses) must not parse as ads.
        assert!(parse_advertisement(&cv("pitch", 0.5)).is_none());
    }
}
