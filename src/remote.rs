//! `gorgon remote <group>` — expose this machine's named signals to a group and
//! present every other member's signals as a native virtual audio device.
//!
//! Phase 1 (this file): **outputs only.** We capture our exposed OUTPUT signals
//! from our physical input channels and stream them to every group member; each
//! peer we hear an advertisement from is materialized as a virtual SOURCE device
//! (their outputs become its channels) that local apps can record from. Exposed
//! INPUTS, the second "into-inputs" port, multi-summing, and live config reload
//! arrive in later phases.
//!
//! Membership is the static roster of Tailscale IPs in `[groups.<name>]`, plus a
//! periodic advertisement (`osc_msg::advertisement`) so each side learns the
//! other's device name and port layout. Everything runs in one `select!` loop so
//! all per-member state (jitter buffers, virtual devices) lives in a single task
//! with no locks — mirroring `stream.rs`'s recv loop.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use cpal::traits::{DeviceTrait, StreamTrait};
use cpal::{BufferSize, SampleRate, StreamConfig};
use ringbuf::{
    traits::{Consumer, Observer, Producer, Split},
    HeapProd, HeapRb,
};
use tokio::net::UdpSocket;
use tracing::{info, warn};

use crate::audio;
use crate::config::Config;
use crate::jitter::JitterBuffer;
use crate::network;
use crate::osc_msg::{self, Advertisement};
use crate::packet::AudioPacket;
use crate::transport;
use crate::vdev::{self, VDevice};

/// Frames per UDP packet (matches `stream.rs`: 64 @ 48 kHz ≈ 1.33 ms).
const FRAMES_PER_PACKET: u8 = 64;
/// Per-device ring capacity in frames (~85 ms @ 48 kHz).
const RING_FRAMES: usize = 4096;
/// How often we re-announce our device + ports to the group.
const ADVERT_INTERVAL: Duration = Duration::from_millis(1500);
/// Drop a member we haven't heard an advertisement from for this long.
const MEMBER_TIMEOUT: Duration = Duration::from_secs(5);

/// A remote member we've learned about and are presenting as a virtual device.
struct Member {
    /// Stable per-run id; a change means the peer restarted (→ rebuild device).
    node_id: u128,
    device_name: String,
    jitter: JitterBuffer,
    /// Drained audio is pushed here; the virtual device plays it out to apps.
    feed: HeapProd<f32>,
    /// Kept alive for the member's lifetime — dropping it removes the PW node.
    _device: VDevice,
    last_seen: Instant,
}

pub async fn run(cfg: &Config, group_name: &str, osc_socket: Arc<UdpSocket>) -> Result<()> {
    // --- Resolve group + our identity --------------------------------------
    let group = cfg
        .groups
        .get(group_name)
        .with_context(|| format!("no [groups.{group_name}] section in config"))?;
    let device_name = cfg.device_name.clone().unwrap_or_else(hostname);
    let node_id = gen_node_id();

    let member_set: HashSet<IpAddr> = group.members.iter().copied().collect();
    let audio_dests: Vec<SocketAddr> = group
        .members
        .iter()
        .map(|ip| SocketAddr::new(*ip, cfg.stream_port))
        .collect();
    let advert_dests: Vec<SocketAddr> = group
        .members
        .iter()
        .map(|ip| SocketAddr::new(*ip, cfg.bind_port))
        .collect();

    info!("remote: joining group '{group_name}' as device '{device_name}' (node {node_id:032x})");
    info!(
        "  roster: {}",
        group
            .members
            .iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // --- Open the physical input; capture our exposed outputs --------------
    let in_dev = audio::find_input_device(cfg.audio.input_device.as_deref())?;
    info!("input device: {}", in_dev.name()?);
    let in_channels = match cfg.audio.input_channels {
        Some(c) => c,
        None => audio::max_input_channels(&in_dev)?,
    };
    let sample_rate = audio::preferred_sample_rate(&in_dev)?;

    // Our exposed outputs, in ordinal order, are the input channels we transmit.
    if cfg.remote.outputs.is_empty() {
        bail!("no [[remote.outputs]] configured — nothing to expose to the group");
    }
    for o in &cfg.remote.outputs {
        if o.channel >= in_channels {
            bail!(
                "output '{}' channel {} exceeds input channels ({in_channels})",
                o.name,
                o.channel
            );
        }
    }
    let send_channels: Vec<u16> = cfg.remote.outputs.iter().map(|o| o.channel).collect();
    info!(
        "exposing {} output(s): {}",
        cfg.remote.outputs.len(),
        cfg.remote
            .outputs
            .iter()
            .map(|o| format!("{}(in{})", o.name, o.channel))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let in_config = StreamConfig {
        channels: in_channels,
        sample_rate: SampleRate(sample_rate),
        buffer_size: BufferSize::Default,
    };
    let (cap_prod, cap_cons) = HeapRb::<f32>::new(RING_FRAMES * in_channels as usize).split();
    let input_stream = audio::build_input_stream(&in_dev, &in_config, cap_prod)?;
    input_stream.play()?;

    // --- Sockets + our advertisement ---------------------------------------
    let stream_bind = SocketAddr::from(([0, 0, 0, 0], cfg.stream_port));
    let stream_socket = Arc::new(UdpSocket::bind(stream_bind).await?);
    info!("audio socket bound to {stream_bind}");

    let my_ad = osc_msg::advertisement(group_name, node_id, &device_name, &cfg.remote);

    // --- Send task: our outputs → every member ----------------------------
    // One packet per block, sent to all members (everyone records the same
    // device), so we encode once and fan it out — see `stream.rs` send task.
    let send_socket = Arc::clone(&stream_socket);
    let send_task = tokio::spawn(async move {
        let mut cap_cons = cap_cons;
        let mut seq: u32 = 0;
        let n_samples = FRAMES_PER_PACKET as usize * in_channels as usize;
        let mut block = vec![0f32; n_samples];

        loop {
            while cap_cons.occupied_len() < n_samples {
                tokio::task::yield_now().await;
            }
            if cap_cons.pop_slice(&mut block) < n_samples {
                continue;
            }
            let pkt = transport::packetize(
                &block,
                in_channels as usize,
                &send_channels,
                FRAMES_PER_PACKET,
                seq,
                sample_rate as u16,
            );
            let encoded = pkt.encode();
            for dest in &audio_dests {
                let _ = send_socket.send_to(&encoded, dest).await;
            }
            seq = seq.wrapping_add(1);
        }
    });

    // --- Event loop: ads in/out, audio in, drain to devices, timeouts ------
    let mut roster: HashMap<IpAddr, Member> = HashMap::new();

    let mut ad_tick = tokio::time::interval(ADVERT_INTERVAL);
    let packet_dur_us = (FRAMES_PER_PACKET as u64 * 1_000_000) / sample_rate as u64;
    let mut drain_tick = tokio::time::interval(Duration::from_micros(packet_dur_us));
    drain_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stale_tick = tokio::time::interval(Duration::from_secs(1));

    let mut osc_buf = vec![0u8; 4096];
    let mut audio_buf = vec![0u8; AudioPacket::wire_len(FRAMES_PER_PACKET, 64)];

    let ctrlc = tokio::signal::ctrl_c();
    tokio::pin!(ctrlc);

    info!("remote running — press Ctrl-C to stop");

    loop {
        tokio::select! {
            _ = &mut ctrlc => {
                info!("shutting down remote");
                break;
            }

            // Re-announce our device + ports (first tick fires immediately).
            _ = ad_tick.tick() => {
                network::broadcast(&osc_socket, &advert_dests, &my_ad).await;
            }

            // Advertisement in → learn / refresh a member.
            r = osc_socket.recv_from(&mut osc_buf) => {
                if let Ok((len, from)) = r {
                    if let Ok((_, packet)) = rosc::decoder::decode_udp(&osc_buf[..len]) {
                        if let Some(ad) = osc_msg::parse_advertisement(&packet) {
                            handle_advertisement(
                                &mut roster, ad, from.ip(), group_name, node_id, &member_set,
                            );
                        }
                    }
                }
            }

            // Audio in → feed the sending member's jitter buffer.
            r = stream_socket.recv_from(&mut audio_buf) => {
                if let Ok((len, from)) = r {
                    if let Some(member) = roster.get_mut(&from.ip()) {
                        if let Some(pkt) = AudioPacket::decode(&audio_buf[..len]) {
                            let out = transport::ingest(&mut member.jitter, pkt);
                            if out.newly_primed {
                                info!("member '{}' primed — playing out", member.device_name);
                            }
                        }
                    }
                }
            }

            // Drain every primed member one packet into its virtual device.
            _ = drain_tick.tick() => {
                for member in roster.values_mut() {
                    if !member.jitter.primed {
                        continue;
                    }
                    if let Some((samples, _src_ch)) = member.jitter.drain_next() {
                        // Drop on overrun: nothing is recording the device yet.
                        let _ = member.feed.push_slice(&samples);
                    }
                }
            }

            // Forget members that stopped advertising (quit / unreachable).
            _ = stale_tick.tick() => {
                let now = Instant::now();
                roster.retain(|ip, m| {
                    let keep = now.duration_since(m.last_seen) < MEMBER_TIMEOUT;
                    if !keep {
                        info!("member '{}' ({ip}) timed out — removing device", m.device_name);
                    }
                    keep
                });
            }
        }
    }

    send_task.abort();
    drop(input_stream);
    Ok(())
}

/// Apply an incoming advertisement: ignore foreign groups, our own echo, and
/// non-roster sources; refresh a known member; (re)build the virtual device for
/// a new member or one that restarted (its `node_id` changed).
fn handle_advertisement(
    roster: &mut HashMap<IpAddr, Member>,
    ad: Advertisement,
    src_ip: IpAddr,
    group_name: &str,
    my_node_id: u128,
    member_set: &HashSet<IpAddr>,
) {
    if ad.group != group_name || ad.node_id == my_node_id || !member_set.contains(&src_ip) {
        return;
    }

    // Known member, same run → just refresh liveness.
    if let Some(existing) = roster.get_mut(&src_ip) {
        if existing.node_id == ad.node_id {
            existing.last_seen = Instant::now();
            return;
        }
    }

    let n_out = ad.outputs.len() as u32;
    if n_out == 0 {
        return; // member exposes nothing to record
    }

    // Build the virtual SOURCE device: its channels are the member's outputs.
    let (feed, cons) = HeapRb::<f32>::new(RING_FRAMES * n_out as usize).split();
    let device = match vdev::source(&ad.device_name, n_out, cons) {
        Ok(d) => d,
        Err(e) => {
            warn!("could not create virtual device '{}': {e:#}", ad.device_name);
            return;
        }
    };

    info!(
        "member '{}' ({src_ip}) joined — device '{}' [{}]",
        ad.device_name,
        ad.device_name,
        ad.outputs
            .iter()
            .enumerate()
            .map(|(i, name)| format!("AUX{i}={name}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Replacing any prior entry drops its old VDevice, tearing down the node.
    roster.insert(
        src_ip,
        Member {
            node_id: ad.node_id,
            device_name: ad.device_name,
            jitter: JitterBuffer::new(),
            feed,
            _device: device,
            last_seen: Instant::now(),
        },
    );
}

/// A stable-enough per-run id to distinguish our own advertisement from peers'.
/// Wall-clock nanoseconds plus the pid differ across machines and restarts; we
/// only need it to not collide with another live member, not cryptographic
/// uniqueness.
fn gen_node_id() -> u128 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let pid = std::process::id() as u128;
    nanos ^ (pid << 96) ^ pid.wrapping_mul(0x9E37_79B9_7F4A_7C15)
}

/// The OS hostname, used as the default device name when none is configured.
fn hostname() -> String {
    let mut buf = [0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc == 0 {
        let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..end]).into_owned()
    } else {
        "gorgon".to_string()
    }
}
