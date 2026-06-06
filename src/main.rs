mod audio;
mod blackhole;
mod config;
mod jitter;
mod network;
mod osc_msg;
mod packet;
mod remote;
mod resample;
mod stream;
mod transport;
mod vdev;

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

    /// Stream raw PCM audio to/from all peers via the ES-9 (or any audio device)
    Stream {
        /// Input device name (substring match, default: system default)
        #[arg(long)]
        input_device: Option<String>,

        /// Output device name (substring match, default: system default)
        #[arg(long)]
        output_device: Option<String>,

        /// Number of channels for both input and output (overridden by --input-channels / --output-channels)
        #[arg(long)]
        channels: Option<u16>,

        /// Number of input channels to capture (default: device maximum)
        #[arg(long)]
        input_channels: Option<u16>,

        /// Number of output channels to play back (default: device maximum)
        #[arg(long)]
        output_channels: Option<u16>,

        /// List available audio devices and exit
        #[arg(long)]
        list_devices: bool,
    },

    /// Join a group: expose your configured signals and present each peer's
    /// signals as a native virtual audio device
    Remote {
        /// Group to join — a `[groups.<name>]` section in the config
        group: String,
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

        Command::Stream { input_device, output_device, channels, input_channels, output_channels, list_devices } => {
            if list_devices {
                audio::list_devices()?;
            } else {
                info!("stream port: {}", cfg.stream_port);
                let in_ch  = input_channels.or(channels);
                let out_ch = output_channels.or(channels);
                stream::run(&cfg, input_device, output_device, in_ch, out_ch).await?;
            }
        }

        Command::Remote { group } => {
            remote::run(&cfg, &group, Arc::clone(&socket)).await?;
        }
    }

    Ok(())
}
