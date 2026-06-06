use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

#[derive(Debug, Deserialize)]
pub struct Config {
    pub bind_port: u16,
    #[serde(default = "default_stream_port")]
    pub stream_port: u16,
    #[serde(default)]
    pub audio: Audio,
    // Defaulted so a remote-only config (no [[peers]]) still loads; the
    // stream/cv/gate/note commands keep reading the same field.
    #[serde(default)]
    pub peers: Vec<Peer>,

    /// How this machine appears as a virtual device on other group members.
    /// Falls back to the hostname when omitted.
    #[serde(default)]
    pub device_name: Option<String>,

    /// The named signals this machine exposes to its groups.
    #[serde(default)]
    pub remote: Remote,

    /// Named groups, each a static roster of member Tailscale IPs.
    #[serde(default)]
    pub groups: BTreeMap<String, Group>,
}

fn default_stream_port() -> u16 {
    9001
}

/// Local audio device settings. All fields are optional; missing values fall
/// back to CLI flags and then to device defaults / maximums.
#[derive(Debug, Deserialize, Default)]
pub struct Audio {
    /// Input device name (substring match).
    pub input_device: Option<String>,
    /// Output device name (substring match).
    pub output_device: Option<String>,
    /// Number of input channels to capture (default: device maximum).
    pub input_channels: Option<u16>,
    /// Number of output channels to open (default: device maximum).
    pub output_channels: Option<u16>,
    /// Playout buffer depth in milliseconds — how much audio to hold before
    /// playing, trading latency for resilience to network jitter (default: 40).
    pub jitter_ms: Option<u16>,
    /// macOS only: the BlackHole device the `remote` command multiplexes the
    /// group onto (substring match). Defaults to "BlackHole" when omitted.
    pub mac_device: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Peer {
    pub name: String,
    pub addr: SocketAddr,

    /// Local input channels (0-based) to capture and send to this peer, in
    /// order. The packet sent to this peer has `send.len()` channels.
    /// Defaults to all captured input channels.
    #[serde(default)]
    pub send: Option<Vec<u16>>,

    /// Routing for audio received from this peer: `[incoming_channel, my_output_channel]`
    /// pairs. Multiple incoming channels may sum into the same output.
    /// Defaults to a positional mapping (incoming channel i -> output i).
    #[serde(default)]
    pub recv: Option<Vec<(u16, u16)>>,
}

/// Signals this machine exposes to a group, split by direction.
///
/// An *output* is a signal we send: it is sourced from one of our local input
/// channels and appears as a capture channel on our device on every peer. An
/// *input* is a signal we accept: peers playing into our device on their
/// machines send to it, and it lands on one of our local output channels.
#[derive(Debug, Deserialize, Default, Clone)]
pub struct Remote {
    #[serde(default)]
    pub outputs: Vec<ExposedPort>,
    #[serde(default)]
    pub inputs: Vec<ExposedPort>,
}

/// A named exposed signal bound to a local physical channel index. For an
/// output, `channel` is a local input channel (the source); for an input, a
/// local output channel (the sink). A port's position in its list is its
/// ordinal — the channel order used on the wire and advertised to peers.
#[derive(Debug, Deserialize, Clone)]
pub struct ExposedPort {
    pub name: String,
    pub channel: u16,
}

/// A named group: the static roster of member Tailscale IPs. Audio is sent to
/// each member at `stream_port`; advertisements travel on `bind_port`.
#[derive(Debug, Deserialize, Clone)]
pub struct Group {
    pub members: Vec<IpAddr>,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}
