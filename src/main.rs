mod config;
mod network;
mod osc_msg;

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use clap::{Parser, Subcommand};
use tokio::net::UdpSocket;
use tracing::{info, Level};

#[derive(Parser)]
#[command(name = "gorgon", about = "P2P OSC control-voltage messenger")]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Listen for incoming OSC messages and print them
    Listen,

    /// Send a CV value to all peers: /cv/<channel> <value 0.0-1.0>
    Cv {
        channel: String,
        value: f32,
    },

    /// Send a gate on/off to all peers: /gate/<channel> <on|off>
    Gate {
        channel: String,
        #[arg(value_parser = parse_on_off)]
        on: bool,
    },

    /// Send a MIDI note number to all peers: /note/<channel> <0-127>
    Note {
        channel: String,
        midi_note: i32,
    },
}

fn parse_on_off(s: &str) -> Result<bool, String> {
    match s {
        "on" | "1" | "true" => Ok(true),
        "off" | "0" | "false" => Ok(false),
        _ => Err(format!("expected on|off, got '{s}'")),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .init();

    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;

    let bind_addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, cfg.bind_port));
    let socket = Arc::new(UdpSocket::bind(bind_addr).await?);
    info!("bound to {bind_addr}");

    let peer_addrs: Vec<SocketAddr> = cfg.peers.iter().map(|p| p.addr).collect();
    info!(
        "peers: {}",
        cfg.peers
            .iter()
            .map(|p| format!("{} ({})", p.name, p.addr))
            .collect::<Vec<_>>()
            .join(", ")
    );

    match cli.command {
        Command::Listen => {
            info!("listening for OSC — press Ctrl-C to stop");
            let socket = Arc::clone(&socket);
            network::receive_loop(socket, |from, packet| {
                println!("{from}  {}", osc_msg::describe(&packet));
            })
            .await;
        }

        Command::Cv { channel, value } => {
            let packet = osc_msg::cv(&channel, value);
            network::broadcast(&socket, &peer_addrs, &packet).await;
            info!("sent /cv/{channel} {value}");
        }

        Command::Gate { channel, on } => {
            let packet = osc_msg::gate(&channel, on);
            network::broadcast(&socket, &peer_addrs, &packet).await;
            info!("sent /gate/{channel} {}", if on { "on" } else { "off" });
        }

        Command::Note { channel, midi_note } => {
            let packet = osc_msg::note(&channel, midi_note);
            network::broadcast(&socket, &peer_addrs, &packet).await;
            info!("sent /note/{channel} {midi_note}");
        }
    }

    Ok(())
}
