use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub bind_port: u16,
    #[serde(default = "default_stream_port")]
    pub stream_port: u16,
    pub peers: Vec<Peer>,
}

fn default_stream_port() -> u16 {
    9001
}

#[derive(Debug, Deserialize, Clone)]
pub struct Peer {
    pub name: String,
    pub addr: SocketAddr,
}

impl Config {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }
}
