/// Helpers for building and interpreting the OSC messages gorgon uses.
///
/// Address convention:
///   /cv/<channel>   f32  — continuous control value, conventionally 0.0 – 1.0
///   /gate/<channel> i32  — 0 = off, 1 = on
///   /note/<channel> i32  — MIDI note number (0-127)
///   /raw            any  — pass arbitrary OSC through unchanged
use rosc::{OscMessage, OscPacket, OscType};

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
