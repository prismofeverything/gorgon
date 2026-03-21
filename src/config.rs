use serde::Deserialize;
use std::net::SocketAddr;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub bind_port: u16,
    pub peers: Vec<Peer>,
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
