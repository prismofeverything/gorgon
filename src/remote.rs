//! `gorgon remote <group>` — expose this machine's named signals to a group and
//! present every other member's signals as a native virtual audio device.
//!
//! Full duplex. From *our* machine's point of view, each peer M has up to four
//! audio flows, kept apart by destination UDP port and demuxed by source IP:
//!
//!   1. **M's outputs → us** (`stream_port`): we feed them into a virtual SOURCE
//!      device named after M — local apps record M's signals.
//!   2. **our outputs → M** (`stream_port`): we capture our exposed outputs from
//!      our physical inputs and broadcast them to every member.
//!   3. **us → M's inputs** (`stream_port+1`): local apps play into a virtual
//!      SINK device named after M; we drain it and send it to M.
//!   4. **M → our inputs** (`stream_port+1`): M played into *its* sink for us; we
//!      sum what arrives onto our physical output channels.
//!
//! Flows 1–2 are the Phase-1 outputs path; 3–4 are the inputs path added here.
//! Membership is the static roster in `[groups.<name>]` plus periodic
//! advertisements (`osc_msg::advertisement`). Everything runs in one `select!`
//! loop, so all per-member state lives in a single task with no locks.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use cpal::traits::{DeviceTrait, StreamTrait};
use cpal::{BufferSize, SampleRate, StreamConfig};
use ringbuf::{
    traits::{Consumer, Observer, Producer, Split},
    HeapCons, HeapProd, HeapRb,
};
use tokio::net::UdpSocket;
use tracing::{info, warn};

use crate::audio;
use crate::blackhole;
use crate::config::{Config, ExposedPort};
use crate::jitter::Playout;
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

/// A remote member we've learned about and are presenting as virtual device(s).
#[cfg_attr(target_os = "macos", allow(dead_code))]
struct Member {
    /// Stable per-run id; a change means the peer restarted (→ rebuild devices).
    node_id: u128,
    device_name: String,

    // Flow 1 — peer's outputs → their SOURCE device on our machine.
    out_play: Playout,
    feed: Option<HeapProd<f32>>, // push drained audio here; None if peer has no outputs
    source_width: usize,         // peer's output channel count (= feed-ring + packet width)
    _source: Option<VDevice>,

    // Flow 3 — local apps → peer's SINK device → drained and sent to the peer.
    intake: Option<HeapCons<f32>>, // drain here and send; None if peer has no inputs
    _sink: Option<VDevice>,
    peer_inputs: usize, // channels the peer accepts (= sink channels = packets we send it)
    in_seq: u32,

    // Flow 4 — peer playing into OUR inputs → summed onto our physical outputs.
    return_play: Playout,

    last_seen: Instant,
}

/// Entry point: dispatch to the platform implementation. The Linux path makes a
/// PipeWire device per peer; the macOS path multiplexes the whole group onto one
/// BlackHole device. Both compile on every platform (so it all type-checks), but
/// only the matching one ever runs.
pub async fn run(cfg: &Config, group_name: &str, osc_socket: Arc<UdpSocket>) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        run_macos(cfg, group_name, osc_socket).await
    }
    #[cfg(not(target_os = "macos"))]
    {
        run_linux(cfg, group_name, osc_socket).await
    }
}

#[cfg_attr(target_os = "macos", allow(dead_code))]
async fn run_linux(cfg: &Config, group_name: &str, osc_socket: Arc<UdpSocket>) -> Result<()> {
    // --- Resolve group + our identity --------------------------------------
    let group = cfg
        .groups
        .get(group_name)
        .with_context(|| format!("no [groups.{group_name}] section in config"))?;
    if cfg.remote.outputs.is_empty() && cfg.remote.inputs.is_empty() {
        bail!("config exposes no [[remote.outputs]] or [[remote.inputs]] — nothing to share");
    }
    let device_name = cfg.device_name.clone().unwrap_or_else(hostname);
    let node_id = gen_node_id();
    let in_port = cfg
        .stream_port
        .checked_add(1)
        .context("stream_port must be < 65535 (we use stream_port+1 for the inputs flow)")?;

    let member_set: HashSet<IpAddr> = group.members.iter().copied().collect();
    let out_dests: Vec<SocketAddr> = group
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
        group.members.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(", ")
    );

    // --- Physical input: capture our exposed outputs (flow 2) --------------
    let in_dev = audio::find_input_device(cfg.audio.input_device.as_deref())?;
    info!("input device:  {}", in_dev.name()?);
    let in_channels = match cfg.audio.input_channels {
        Some(c) => c,
        None => audio::max_input_channels(&in_dev)?,
    };
    let sample_rate = audio::preferred_sample_rate(&in_dev)?;
    // Playout buffer depth (jitter-buffer prime) from `audio.jitter_ms` — the
    // same latency/stability knob the point-to-point `stream` path uses.
    let prime = transport::prime_packets(cfg.audio.jitter_ms, sample_rate, FRAMES_PER_PACKET);
    // Target depth (frames) the drain loop keeps queued in each output ring — the
    // occupancy set-point that paces playout to the device clock (see `refill`).
    let target_frames = ((cfg.audio.jitter_ms.unwrap_or(transport::DEFAULT_JITTER_MS).max(5) as usize)
        * sample_rate as usize / 1000)
        .min(RING_FRAMES - FRAMES_PER_PACKET as usize);
    info!(
        "playout buffer: ~{prime} packets ({} ms target)",
        cfg.audio.jitter_ms.unwrap_or(transport::DEFAULT_JITTER_MS)
    );
    for o in &cfg.remote.outputs {
        if o.channel >= in_channels {
            bail!("output '{}' channel {} exceeds input channels ({in_channels})", o.name, o.channel);
        }
    }
    let send_channels: Vec<u16> = cfg.remote.outputs.iter().map(|o| o.channel).collect();
    if !send_channels.is_empty() {
        info!(
            "exposing {} output(s): {}",
            cfg.remote.outputs.len(),
            cfg.remote.outputs.iter().map(|o| format!("{}(in{})", o.name, o.channel)).collect::<Vec<_>>().join(", ")
        );
    }

    let in_config = StreamConfig {
        channels: in_channels,
        sample_rate: SampleRate(sample_rate),
        buffer_size: BufferSize::Default,
    };
    let (cap_prod, cap_cons) = HeapRb::<f32>::new(RING_FRAMES * in_channels as usize).split();
    let input_stream = audio::build_input_stream(&in_dev, &in_config, cap_prod)?;
    input_stream.play()?;

    // --- Physical output: only needed when we accept inputs (flow 4) -------
    let accept_inputs = !cfg.remote.inputs.is_empty();
    // packet-channel k (a peer playing into our input ordinal k) -> our output channel.
    let my_in_pairs: Vec<(u16, u16)> = cfg
        .remote
        .inputs
        .iter()
        .enumerate()
        .map(|(k, p)| (k as u16, p.channel))
        .collect();
    let mut out_channels = 0usize;

    let (mut play_prod, _output_stream) = if accept_inputs {
        let in_name = cfg.audio.input_device.clone();
        let out_name = cfg.audio.output_device.clone();
        // Reuse the input device handle when it's the same physical device
        // (opening the same ALSA PCM twice fails) — see stream.rs.
        let out_dev = if out_name.is_some() && out_name == in_name {
            in_dev.clone()
        } else {
            audio::find_output_device(out_name.as_deref())?
        };
        info!("output device: {}", out_dev.name()?);
        let oc = match cfg.audio.output_channels {
            Some(c) => c,
            None => audio::max_output_channels(&out_dev)?,
        };
        for p in &cfg.remote.inputs {
            if p.channel >= oc {
                bail!("input '{}' channel {} exceeds output channels ({oc})", p.name, p.channel);
            }
        }
        out_channels = oc as usize;
        info!(
            "accepting {} input(s): {}",
            cfg.remote.inputs.len(),
            cfg.remote.inputs.iter().map(|p| format!("{}(out{})", p.name, p.channel)).collect::<Vec<_>>().join(", ")
        );
        let out_config = StreamConfig {
            channels: oc,
            sample_rate: SampleRate(sample_rate),
            buffer_size: BufferSize::Default,
        };
        let (pp, play_cons) = HeapRb::<f32>::new(RING_FRAMES * oc as usize).split();
        let os = audio::build_output_stream(&out_dev, &out_config, play_cons)?;
        os.play()?;
        (Some(pp), Some(os))
    } else {
        (None, None)
    };

    // --- Sockets + our advertisement ---------------------------------------
    let stream_socket = Arc::new(UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], cfg.stream_port))).await?);
    let input_socket = Arc::new(UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], in_port))).await?);
    info!("audio sockets bound to 0.0.0.0:{} (outputs) and 0.0.0.0:{in_port} (inputs)", cfg.stream_port);

    let my_ad = osc_msg::advertisement(group_name, node_id, &device_name, &cfg.remote);

    // --- Send task: our outputs → every member (flow 2) --------------------
    // One packet per block, fanned out to all members (everyone records the
    // same device), so we encode once. Skipped entirely if we expose no outputs.
    let send_task = if !send_channels.is_empty() {
        let send_socket = Arc::clone(&stream_socket);
        Some(tokio::spawn(async move {
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
                    &block, in_channels as usize, &send_channels, FRAMES_PER_PACKET, seq, sample_rate as u16,
                );
                let encoded = pkt.encode();
                for dest in &out_dests {
                    let _ = send_socket.send_to(&encoded, dest).await;
                }
                seq = seq.wrapping_add(1);
            }
        }))
    } else {
        None
    };

    // --- Event loop --------------------------------------------------------
    let mut roster: HashMap<IpAddr, Member> = HashMap::new();
    let send2_socket = Arc::clone(&input_socket); // flow 3 sends; recv is on `input_socket`
    let frames_usize = FRAMES_PER_PACKET as usize;

    let mut ad_tick = tokio::time::interval(ADVERT_INTERVAL);
    let packet_dur_us = (FRAMES_PER_PACKET as u64 * 1_000_000) / sample_rate as u64;
    // Tick at twice the packet rate so the occupancy-paced refill tracks the
    // device's consumption closely (it pushes only as the ring frees space).
    let mut drain_tick = tokio::time::interval(Duration::from_micros((packet_dur_us / 2).max(1)));
    drain_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stale_tick = tokio::time::interval(Duration::from_secs(1));

    let mut osc_buf = vec![0u8; 4096];
    let mut out_buf = vec![0u8; AudioPacket::wire_len(FRAMES_PER_PACKET, 64)];
    let mut in_buf = vec![0u8; AudioPacket::wire_len(FRAMES_PER_PACKET, 64)];
    let mut scratch: Vec<f32> = Vec::new();

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
                            handle_advertisement(&mut roster, ad, from.ip(), group_name, node_id, &member_set, prime);
                        }
                    }
                }
            }

            // Flow 1 — peer's outputs → their jitter buffer.
            r = stream_socket.recv_from(&mut out_buf) => {
                if let Ok((len, from)) = r {
                    if let Some(member) = roster.get_mut(&from.ip()) {
                        if let Some(pkt) = AudioPacket::decode(&out_buf[..len]) {
                            if member.out_play.insert(pkt) {
                                info!("member '{}' outputs primed", member.device_name);
                            }
                        }
                    }
                }
            }

            // Flow 4 — peer played into our inputs → their return jitter buffer.
            r = input_socket.recv_from(&mut in_buf) => {
                if let Ok((len, from)) = r {
                    if accept_inputs {
                        if let Some(member) = roster.get_mut(&from.ip()) {
                            if let Some(pkt) = AudioPacket::decode(&in_buf[..len]) {
                                member.return_play.insert(pkt);
                            }
                        }
                    }
                }
            }

            _ = drain_tick.tick() => {
                // (a) Flow 1: keep each peer's source-device ring topped up to the
                // target depth, paced by how fast that device drains it.
                for member in roster.values_mut() {
                    let Member { out_play, feed, source_width, .. } = member;
                    if let Some(feed) = feed.as_mut() {
                        let width = *source_width;
                        refill(feed, target_frames * width, frames_usize * width, || {
                            out_play.next_block().map(|(samples, _)| samples)
                        });
                    }
                }

                // (b) Flow 4: keep the physical-output ring topped up to the target
                // depth with the peers' summed returns, paced by the output clock.
                if let Some(pp) = play_prod.as_mut() {
                    let block_len = frames_usize * out_channels;
                    refill(pp, target_frames * out_channels, block_len, || {
                        let mut out_block = vec![0f32; block_len];
                        let mut produced = false;
                        for member in roster.values_mut() {
                            if let Some((samples, src_ch)) = member.return_play.next_block() {
                                transport::sum_into_output(
                                    &mut out_block, &samples, src_ch as usize, out_channels, &my_in_pairs, frames_usize,
                                );
                                produced = true;
                            }
                        }
                        if produced { Some(out_block) } else { None }
                    });
                }

                // (c) Flow 3: drain what local apps played into each peer's sink
                //     and send it to that peer.
                for (ip, member) in roster.iter_mut() {
                    let need = frames_usize * member.peer_inputs;
                    if need == 0 {
                        continue;
                    }
                    let dest = SocketAddr::new(*ip, in_port);
                    let mut seq = member.in_seq;
                    if let Some(intake) = member.intake.as_mut() {
                        while intake.occupied_len() >= need {
                            scratch.resize(need, 0.0);
                            if intake.pop_slice(&mut scratch[..need]) < need {
                                break;
                            }
                            let pkt = AudioPacket {
                                seq,
                                sample_rate: sample_rate as u16,
                                channels: member.peer_inputs as u8,
                                frames: FRAMES_PER_PACKET,
                                samples: scratch[..need].to_vec(),
                            };
                            seq = seq.wrapping_add(1);
                            let _ = send2_socket.send_to(&pkt.encode(), dest).await;
                        }
                    }
                    member.in_seq = seq;
                }
            }

            // Forget members that stopped advertising (quit / unreachable).
            _ = stale_tick.tick() => {
                let now = Instant::now();
                roster.retain(|ip, m| {
                    let keep = now.duration_since(m.last_seen) < MEMBER_TIMEOUT;
                    if !keep {
                        info!("member '{}' ({ip}) timed out — removing device(s)", m.device_name);
                    }
                    keep
                });
            }
        }
    }

    if let Some(t) = send_task {
        t.abort();
    }
    drop(input_stream);
    drop(_output_stream);
    Ok(())
}

/// Apply an incoming advertisement: ignore foreign groups, our own echo, and
/// non-roster sources; refresh a known member; (re)build the virtual device(s)
/// for a new member or one that restarted (its `node_id` changed). A peer's
/// outputs become a SOURCE device we record; its inputs become a SINK device we
/// play into.
#[cfg_attr(target_os = "macos", allow(dead_code))]
fn handle_advertisement(
    roster: &mut HashMap<IpAddr, Member>,
    ad: Advertisement,
    src_ip: IpAddr,
    group_name: &str,
    my_node_id: u128,
    member_set: &HashSet<IpAddr>,
    prime: usize,
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

    let n_out = ad.outputs.len();
    let n_in = ad.inputs.len();
    if n_out == 0 && n_in == 0 {
        return; // peer shares nothing
    }

    // Peer's outputs → a SOURCE device we record from.
    let (feed, source) = if n_out > 0 {
        let (prod, cons) = HeapRb::<f32>::new(RING_FRAMES * n_out).split();
        match vdev::source(&ad.device_name, n_out as u32, cons) {
            Ok(d) => (Some(prod), Some(d)),
            Err(e) => {
                warn!("could not create source device '{}': {e:#}", ad.device_name);
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    // Peer's inputs → a SINK device we play into.
    let (intake, sink) = if n_in > 0 {
        let (prod, cons) = HeapRb::<f32>::new(RING_FRAMES * n_in).split();
        match vdev::sink(&ad.device_name, n_in as u32, prod) {
            Ok(d) => (Some(cons), Some(d)),
            Err(e) => {
                warn!("could not create sink device '{}': {e:#}", ad.device_name);
                (None, None)
            }
        }
    } else {
        (None, None)
    };

    info!(
        "member '{}' ({src_ip}) joined — record [{}] / play [{}]",
        ad.device_name,
        labelled(&ad.outputs),
        labelled(&ad.inputs),
    );

    // Replacing any prior entry drops its old VDevices, tearing down the nodes.
    roster.insert(
        src_ip,
        Member {
            node_id: ad.node_id,
            device_name: ad.device_name,
            out_play: Playout::new(prime),
            feed,
            source_width: n_out,
            _source: source,
            intake,
            _sink: sink,
            peer_inputs: n_in,
            in_seq: 0,
            return_play: Playout::new(prime),
            last_seen: Instant::now(),
        },
    );
}

/// Render a signal list as the `AUXn=name` mapping gorgon prints (the OS shows
/// the channels as AUX0, AUX1, … while gorgon keeps the human names).
fn labelled(names: &[String]) -> String {
    names
        .iter()
        .enumerate()
        .map(|(i, name)| format!("AUX{i}={name}"))
        .collect::<Vec<_>>()
        .join(", ")
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

/// Top up an output ring toward `target_occ` samples, pushing whole
/// `block_len`-sample blocks supplied by `next`. Pacing by the ring's occupancy
/// locks our push rate to the device draining it — the device only frees space
/// at the hardware clock, so the ring settles near `target_occ` instead of
/// drifting full (drops) or empty (glitches) the way a free-running timer would.
/// Stops at the target, when no whole block fits, or when `next` returns `None`
/// (nothing primed, or the loss ran past the PLC window — let the ring drain).
fn refill(
    ring: &mut HeapProd<f32>,
    target_occ: usize,
    block_len: usize,
    mut next: impl FnMut() -> Option<Vec<f32>>,
) {
    while ring.vacant_len() >= block_len && ring.occupied_len() < target_occ {
        match next() {
            Some(block) => {
                ring.push_slice(&block);
            }
            None => break,
        }
    }
}

// ===========================================================================
// macOS implementation
//
// macOS can't mint a device per peer, so everything is multiplexed onto one
// BlackHole device (see blackhole.rs). gorgon owns one duplex stream; the four
// audio flows become channel lanes, and the multiplexing happens here in the
// (non-real-time) event loop using blackhole::{place_lane, extract_lane}. The
// channel assignment is printed at startup as a map the user follows in their
// DAW. This compiles on every platform but is only run on macOS.
// ===========================================================================

/// A remote member, as represented on macOS by channel lanes on the shared
/// BlackHole device (rather than per-peer PipeWire nodes).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
struct MacMember {
    node_id: u128,
    device_name: String,
    /// Flow 1 — peer's outputs; drained and written to the `out_lane` channels
    /// (which the user records).
    out_play: Playout,
    out_lane: usize,
    out_width: usize,
    /// Flow 4 — peer playing into our inputs; summed (with other peers) onto our
    /// own inputs' write-lane.
    return_play: Playout,
    /// Flow 3 — what the user plays into this peer (read from `in_lane`), sent on.
    in_lane: usize,
    in_width: usize,
    in_seq: u32,
    last_seen: Instant,
}

#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
async fn run_macos(cfg: &Config, group_name: &str, osc_socket: Arc<UdpSocket>) -> Result<()> {
    let group = cfg
        .groups
        .get(group_name)
        .with_context(|| format!("no [groups.{group_name}] section in config"))?;
    let device_name = cfg.device_name.clone().unwrap_or_else(hostname);
    let node_id = gen_node_id();
    let in_port = cfg
        .stream_port
        .checked_add(1)
        .context("stream_port must be < 65535 (we use stream_port+1 for the inputs flow)")?;
    let mac_device = cfg.audio.mac_device.clone().unwrap_or_else(|| "BlackHole".to_string());
    // Playout buffer depth (jitter-buffer prime) from `audio.jitter_ms`; BlackHole runs at 48 kHz.
    let prime = transport::prime_packets(cfg.audio.jitter_ms, 48_000, FRAMES_PER_PACKET);
    // Target depth (frames) the drain loop keeps queued in the BlackHole ring —
    // the occupancy set-point that paces playout to the device clock (see `refill`).
    let target_frames = ((cfg.audio.jitter_ms.unwrap_or(transport::DEFAULT_JITTER_MS).max(5) as usize)
        * 48_000 / 1000)
        .min(RING_FRAMES - FRAMES_PER_PACKET as usize);

    let member_set: HashSet<IpAddr> = group.members.iter().copied().collect();
    let out_dests: Vec<SocketAddr> = group
        .members
        .iter()
        .map(|ip| SocketAddr::new(*ip, cfg.stream_port))
        .collect();
    let advert_dests: Vec<SocketAddr> = group
        .members
        .iter()
        .map(|ip| SocketAddr::new(*ip, cfg.bind_port))
        .collect();

    info!("remote (macOS): joining '{group_name}' as '{device_name}' (node {node_id:032x})");

    // Open BlackHole and lay out our own channels. Records and playback live in
    // disjoint channel regions so the loopback never feeds back on itself.
    let mut hub = blackhole::Hub::open(&mac_device, RING_FRAMES).with_context(|| {
        format!("could not open BlackHole device '{mac_device}' — is BlackHole installed? https://existential.audio/blackhole/")
    })?;
    let n = hub.channels;
    let mut chans = blackhole::ChannelMap::new(hub.channels, hub.split);
    info!(
        "BlackHole '{mac_device}': {n} ch | record region 0..{} | playback region {}..{n}",
        hub.split, hub.split
    );

    let n_out = cfg.remote.outputs.len();
    let n_in = cfg.remote.inputs.len();
    let my_out_lane = if n_out > 0 {
        let off = chans.alloc_read(n_out)?;
        info!("  YOUR OUTPUTS — PLAY into BlackHole ch {off}..{}: {}", off + n_out, port_names(&cfg.remote.outputs));
        Some(off)
    } else {
        None
    };
    let my_in_lane = if n_in > 0 {
        let off = chans.alloc_write(n_in)?;
        info!("  YOUR INPUTS — RECORD BlackHole ch {off}..{}: {}", off + n_in, port_names(&cfg.remote.inputs));
        Some(off)
    } else {
        None
    };
    let my_in_pairs: Vec<(u16, u16)> = (0..n_in).map(|k| (k as u16, k as u16)).collect();

    let stream_socket = Arc::new(UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], cfg.stream_port))).await?);
    let input_socket = Arc::new(UdpSocket::bind(SocketAddr::from(([0, 0, 0, 0], in_port))).await?);

    let my_ad = osc_msg::advertisement(group_name, node_id, &device_name, &cfg.remote);

    let mut roster: HashMap<IpAddr, MacMember> = HashMap::new();
    let frames = FRAMES_PER_PACKET as usize;
    let full = n * frames; // samples in one full-width packet-sized frame

    let mut ad_tick = tokio::time::interval(ADVERT_INTERVAL);
    let mut drain_tick =
        tokio::time::interval(Duration::from_micros((FRAMES_PER_PACKET as u64 * 1_000_000) / 48_000 / 2));
    drain_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stale_tick = tokio::time::interval(Duration::from_secs(1));

    let mut osc_buf = vec![0u8; 4096];
    let mut out_buf = vec![0u8; AudioPacket::wire_len(FRAMES_PER_PACKET, 64)];
    let mut in_buf = vec![0u8; AudioPacket::wire_len(FRAMES_PER_PACKET, 64)];
    let mut in_frame = vec![0f32; full];
    let mut my_out_seq: u32 = 0;

    let ctrlc = tokio::signal::ctrl_c();
    tokio::pin!(ctrlc);

    info!("remote running — press Ctrl-C to stop");

    loop {
        tokio::select! {
            _ = &mut ctrlc => {
                info!("shutting down remote");
                break;
            }

            _ = ad_tick.tick() => {
                network::broadcast(&osc_socket, &advert_dests, &my_ad).await;
            }

            r = osc_socket.recv_from(&mut osc_buf) => {
                if let Ok((len, from)) = r {
                    if let Ok((_, packet)) = rosc::decoder::decode_udp(&osc_buf[..len]) {
                        if let Some(ad) = osc_msg::parse_advertisement(&packet) {
                            handle_advertisement_macos(&mut roster, &mut chans, ad, from.ip(), group_name, node_id, &member_set, prime);
                        }
                    }
                }
            }

            // Flow 1 — peer outputs.
            r = stream_socket.recv_from(&mut out_buf) => {
                if let Ok((len, from)) = r {
                    if let Some(m) = roster.get_mut(&from.ip()) {
                        if let Some(pkt) = AudioPacket::decode(&out_buf[..len]) {
                            if m.out_play.insert(pkt) {
                                info!("member '{}' outputs primed", m.device_name);
                            }
                        }
                    }
                }
            }

            // Flow 4 — peer playing into our inputs.
            r = input_socket.recv_from(&mut in_buf) => {
                if let Ok((len, from)) = r {
                    if my_in_lane.is_some() {
                        if let Some(m) = roster.get_mut(&from.ip()) {
                            if let Some(pkt) = AudioPacket::decode(&in_buf[..len]) {
                                m.return_play.insert(pkt);
                            }
                        }
                    }
                }
            }

            _ = drain_tick.tick() => {
                // ---- Keep the BlackHole record ring topped up to the target depth
                // with full-width frames (flows 1 + 4), paced by the device clock. ----
                refill(&mut hub.to_device, target_frames * n, full, || {
                    let mut frame = vec![0f32; full];
                    let mut produced = false;
                    for m in roster.values_mut() {
                        if let Some((samples, _)) = m.out_play.next_block() {
                            blackhole::place_lane(&mut frame, n, m.out_lane, m.out_width, &samples);
                            produced = true;
                        }
                    }
                    if let Some(off) = my_in_lane {
                        let mut mixed = vec![0f32; frames * n_in];
                        let mut mixed_any = false;
                        for m in roster.values_mut() {
                            if let Some((samples, src_ch)) = m.return_play.next_block() {
                                transport::sum_into_output(&mut mixed, &samples, src_ch as usize, n_in, &my_in_pairs, frames);
                                mixed_any = true;
                            }
                        }
                        if mixed_any {
                            blackhole::place_lane(&mut frame, n, off, n_in, &mixed);
                            produced = true;
                        }
                    }
                    if produced { Some(frame) } else { None }
                });

                // ---- Read frames the user plays and send them on (flows 2 + 3) ----
                while hub.from_device.occupied_len() >= full {
                    if hub.from_device.pop_slice(&mut in_frame) < full {
                        break;
                    }
                    if let Some(off) = my_out_lane {
                        let pkt = AudioPacket {
                            seq: my_out_seq,
                            sample_rate: 48_000,
                            channels: n_out as u8,
                            frames: FRAMES_PER_PACKET,
                            samples: blackhole::extract_lane(&in_frame, n, off, n_out),
                        };
                        my_out_seq = my_out_seq.wrapping_add(1);
                        let encoded = pkt.encode();
                        for dest in &out_dests {
                            let _ = stream_socket.send_to(&encoded, dest).await;
                        }
                    }
                    for (ip, m) in roster.iter_mut() {
                        if m.in_width == 0 {
                            continue;
                        }
                        let pkt = AudioPacket {
                            seq: m.in_seq,
                            sample_rate: 48_000,
                            channels: m.in_width as u8,
                            frames: FRAMES_PER_PACKET,
                            samples: blackhole::extract_lane(&in_frame, n, m.in_lane, m.in_width),
                        };
                        m.in_seq = m.in_seq.wrapping_add(1);
                        let _ = input_socket.send_to(&pkt.encode(), SocketAddr::new(*ip, in_port)).await;
                    }
                }
            }

            _ = stale_tick.tick() => {
                let now = Instant::now();
                roster.retain(|ip, m| {
                    let keep = now.duration_since(m.last_seen) < MEMBER_TIMEOUT;
                    if !keep {
                        info!("member '{}' ({ip}) timed out", m.device_name);
                    }
                    keep
                });
            }
        }
    }

    Ok(())
}

/// macOS roster update: assign BlackHole channel lanes for a new member and
/// print the map. On restart (same IP, new node_id) we keep the member's lanes
/// to avoid leaking channels — this assumes the peer's port *counts* are stable
/// across a restart (changing them needs both ends to restart).
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn handle_advertisement_macos(
    roster: &mut HashMap<IpAddr, MacMember>,
    chans: &mut blackhole::ChannelMap,
    ad: Advertisement,
    src_ip: IpAddr,
    group_name: &str,
    my_node_id: u128,
    member_set: &HashSet<IpAddr>,
    prime: usize,
) {
    if ad.group != group_name || ad.node_id == my_node_id || !member_set.contains(&src_ip) {
        return;
    }

    if let Some(existing) = roster.get_mut(&src_ip) {
        if existing.node_id == ad.node_id {
            existing.last_seen = Instant::now();
            return;
        }
        // Restart: reuse the existing lanes, reset stream state.
        existing.node_id = ad.node_id;
        existing.device_name = ad.device_name;
        existing.out_play = Playout::new(prime);
        existing.return_play = Playout::new(prime);
        existing.in_seq = 0;
        existing.last_seen = Instant::now();
        return;
    }

    let n_out = ad.outputs.len();
    let n_in = ad.inputs.len();
    if n_out == 0 && n_in == 0 {
        return;
    }

    let out_lane = if n_out > 0 {
        match chans.alloc_write(n_out) {
            Ok(off) => {
                info!("member '{}' ({src_ip}) — RECORD their outputs on BlackHole ch {off}..{}: {}", ad.device_name, off + n_out, ad.outputs.join(", "));
                off
            }
            Err(e) => {
                warn!("no record channels for '{}': {e}", ad.device_name);
                return;
            }
        }
    } else {
        0
    };
    let in_lane = if n_in > 0 {
        match chans.alloc_read(n_in) {
            Ok(off) => {
                info!("member '{}' ({src_ip}) — PLAY into their inputs on BlackHole ch {off}..{}: {}", ad.device_name, off + n_in, ad.inputs.join(", "));
                off
            }
            Err(e) => {
                warn!("no playback channels for '{}': {e}", ad.device_name);
                return;
            }
        }
    } else {
        0
    };

    roster.insert(
        src_ip,
        MacMember {
            node_id: ad.node_id,
            device_name: ad.device_name,
            out_play: Playout::new(prime),
            out_lane,
            out_width: n_out,
            return_play: Playout::new(prime),
            in_lane,
            in_width: n_in,
            in_seq: 0,
            last_seen: Instant::now(),
        },
    );
}

/// Join exposed-port names for the startup channel map.
#[cfg_attr(not(target_os = "macos"), allow(dead_code))]
fn port_names(ports: &[ExposedPort]) -> String {
    ports.iter().map(|p| p.name.as_str()).collect::<Vec<_>>().join(", ")
}
