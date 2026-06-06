use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub bind_port: u16,
    #[serde(default = "default_stream_port")]
    pub stream_port: u16,
    #[serde(default)]
    pub audio: Audio,
    pub peers: Vec<Peer>,
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

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}
