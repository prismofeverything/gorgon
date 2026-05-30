use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{bail, Result};
use cpal::traits::{DeviceTrait, StreamTrait};
use cpal::{BufferSize, SampleRate, StreamConfig};
use ringbuf::{HeapRb, traits::{Consumer, Observer, Producer, Split}};
use tokio::net::UdpSocket;
use tracing::{info, warn};

use crate::audio;
use crate::config::Config;
use crate::jitter::JitterBuffer;
use crate::packet::AudioPacket;
use crate::transport;

/// Number of audio frames packed into each UDP packet.
/// 64 frames @ 48 kHz = ~1.33 ms per packet.
const FRAMES_PER_PACKET: u8 = 64;

/// Ring buffer capacity in frames — covers ~85 ms at 48 kHz.
const RING_FRAMES: usize = 4096;

/// A resolved per-peer audio route.
#[derive(Clone)]
struct Route {
    name: String,
    dest: SocketAddr,      // where to send our audio: peer ip + stream_port
    ip:   IpAddr,          // source ip used to match incoming packets to this peer
    send: Vec<u16>,        // local input channels to transmit, in order
    recv: Vec<(u16, u16)>, // (incoming packet channel, local output channel)
}

pub async fn run(
    cfg: &Config,
    input_device:    Option<String>,
    output_device:   Option<String>,
    in_channels_override:  Option<u16>,
    out_channels_override: Option<u16>,
) -> Result<()> {
    // --- Devices and config -------------------------------------------------
    // Precedence: CLI flag > [audio] config > device default/maximum.

    let in_name  = input_device.or_else(|| cfg.audio.input_device.clone());
    let out_name = output_device.or_else(|| cfg.audio.output_device.clone());

    // A cpal ALSA Device holds both a capture and a playback PCM handle for the
    // device's whole lifetime. When input and output are the same device (e.g.
    // both "ES9"), look it up once and reuse the handle for both streams —
    // looking it up twice would fail, since the second open finds the PCM busy.
    let (in_dev, out_dev) = if in_name.is_some() && in_name == out_name {
        let dev = audio::find_input_device(in_name.as_deref())?;
        (dev.clone(), dev)
    } else {
        (
            audio::find_input_device(in_name.as_deref())?,
            audio::find_output_device(out_name.as_deref())?,
        )
    };

    info!("input device:  {}", in_dev.name()?);
    info!("output device: {}", out_dev.name()?);

    // Probe for the channel count only when it isn't pinned in config/CLI —
    // `unwrap_or` would evaluate (and fail) the probe even when a value is set.
    let in_channels = match in_channels_override.or(cfg.audio.input_channels) {
        Some(c) => c,
        None => audio::max_input_channels(&in_dev)?,
    };
    let out_channels = match out_channels_override.or(cfg.audio.output_channels) {
        Some(c) => c,
        None => audio::max_output_channels(&out_dev)?,
    };
    let sample_rate = audio::preferred_sample_rate(&in_dev)?;

    info!(
        "audio: {} Hz | in {} ch | out {} ch | {} frames/packet",
        sample_rate, in_channels, out_channels, FRAMES_PER_PACKET
    );

    // --- Resolve per-peer routes -------------------------------------------

    let routes: Vec<Route> = cfg
        .peers
        .iter()
        .map(|p| Route {
            name: p.name.clone(),
            dest: SocketAddr::new(p.addr.ip(), cfg.stream_port),
            ip:   p.addr.ip(),
            send: p.send.clone().unwrap_or_else(|| (0..in_channels).collect()),
            recv: p
                .recv
                .clone()
                .unwrap_or_else(|| (0..out_channels).map(|i| (i, i)).collect()),
        })
        .collect();

    // Validate route channel indices against the local device.
    for r in &routes {
        for &ch in &r.send {
            if ch >= in_channels {
                bail!("peer '{}': send channel {ch} exceeds input channels ({in_channels})", r.name);
            }
        }
        for &(_pc, oc) in &r.recv {
            if oc >= out_channels {
                bail!("peer '{}': output channel {oc} exceeds output channels ({out_channels})", r.name);
            }
        }
    }

    for r in &routes {
        info!("route {} ({}): send {:?} | recv {:?}", r.name, r.dest, r.send, r.recv);
    }

    // --- Stream configs -----------------------------------------------------

    let in_config = StreamConfig {
        channels:    in_channels,
        sample_rate: SampleRate(sample_rate),
        buffer_size: BufferSize::Default,
    };
    let out_config = StreamConfig {
        channels:    out_channels,
        sample_rate: SampleRate(sample_rate),
        buffer_size: BufferSize::Default,
    };

    // --- Ring buffers -------------------------------------------------------

    let (cap_prod, cap_cons)   = HeapRb::<f32>::new(RING_FRAMES * in_channels as usize).split();
    let (play_prod, play_cons) = HeapRb::<f32>::new(RING_FRAMES * out_channels as usize).split();

    // --- Audio streams ------------------------------------------------------

    let input_stream  = audio::build_input_stream(&in_dev,  &in_config,  cap_prod)?;
    let output_stream = audio::build_output_stream(&out_dev, &out_config, play_cons)?;

    input_stream.play()?;
    output_stream.play()?;

    // --- Network ------------------------------------------------------------

    let bind_addr = SocketAddr::from(([0, 0, 0, 0], cfg.stream_port));
    let socket    = Arc::new(UdpSocket::bind(bind_addr).await?);
    info!("stream socket bound to {bind_addr}");

    // --- Send task ----------------------------------------------------------
    //
    // Each block of captured frames is re-interleaved into a per-peer packet
    // containing only that peer's `send` channels, in the configured order.

    // Diagnostic counters, logged once a second by the stats task below.
    let sent         = Arc::new(AtomicU64::new(0));
    let recv_ok      = Arc::new(AtomicU64::new(0));
    let recv_unknown = Arc::new(AtomicU64::new(0));
    let recv_bad     = Arc::new(AtomicU64::new(0));
    let resyncs      = Arc::new(AtomicU64::new(0));

    let send_socket  = Arc::clone(&socket);
    let send_routes  = routes.clone();
    let send_sent    = Arc::clone(&sent);
    let frames       = FRAMES_PER_PACKET;
    let frames_usize = frames as usize;
    let in_ch_usize  = in_channels as usize;
    let in_n_samples = frames_usize * in_ch_usize;

    let send_task = tokio::spawn(async move {
        let mut cap_cons = cap_cons;
        let mut seq: u32 = 0;
        let mut block    = vec![0f32; in_n_samples];

        loop {
            while cap_cons.occupied_len() < in_n_samples {
                tokio::task::yield_now().await;
            }
            let popped = cap_cons.pop_slice(&mut block);
            if popped < in_n_samples {
                continue;
            }

            for route in &send_routes {
                let pkt = transport::packetize(
                    &block, in_ch_usize, &route.send, frames, seq, sample_rate as u16,
                );
                let encoded = pkt.encode();
                if let Err(e) = send_socket.send_to(&encoded, route.dest).await {
                    warn!("send → {} ({}): {e}", route.name, route.dest);
                } else {
                    send_sent.fetch_add(1, Ordering::Relaxed);
                }
            }
            seq = seq.wrapping_add(1);
        }
    });

    // --- Receive task -------------------------------------------------------
    //
    // Each peer gets its own jitter buffer, keyed by source IP. On every drain
    // tick all primed buffers are advanced and their routed channels summed
    // into one output block — so several peers can land on the same output.

    let recv_socket    = Arc::clone(&socket);
    let recv_routes    = routes.clone();
    let recv_ok_c      = Arc::clone(&recv_ok);
    let recv_unknown_c = Arc::clone(&recv_unknown);
    let recv_bad_c     = Arc::clone(&recv_bad);
    let resyncs_c      = Arc::clone(&resyncs);
    let packet_dur_us  = (frames as u64 * 1_000_000) / sample_rate as u64;
    let out_ch_usize   = out_channels as usize;
    let out_n_samples  = frames_usize * out_ch_usize;
    let max_packet_len = AudioPacket::wire_len(frames, 64);

    let ip_to_route: HashMap<IpAddr, usize> =
        recv_routes.iter().enumerate().map(|(i, r)| (r.ip, i)).collect();

    let recv_task = tokio::spawn(async move {
        let mut play_prod = play_prod;
        let mut jitters: Vec<JitterBuffer> =
            (0..recv_routes.len()).map(|_| JitterBuffer::new()).collect();
        let mut recv_buf     = vec![0u8; max_packet_len];
        let mut unknown_seen: HashSet<IpAddr> = HashSet::new();
        let mut drain_tick   = tokio::time::interval(Duration::from_micros(packet_dur_us));
        drain_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                result = recv_socket.recv_from(&mut recv_buf) => {
                    match result {
                        Ok((len, from)) => match ip_to_route.get(&from.ip()) {
                            Some(&ri) => match AudioPacket::decode(&recv_buf[..len]) {
                                Some(pkt) => {
                                    let out = transport::ingest(&mut jitters[ri], pkt);
                                    if out.resynced {
                                        resyncs_c.fetch_add(1, Ordering::Relaxed);
                                    }
                                    if out.newly_primed {
                                        info!("peer {} primed — playing out", recv_routes[ri].name);
                                    }
                                    recv_ok_c.fetch_add(1, Ordering::Relaxed);
                                }
                                None => { recv_bad_c.fetch_add(1, Ordering::Relaxed); }
                            },
                            None => {
                                recv_unknown_c.fetch_add(1, Ordering::Relaxed);
                                if unknown_seen.insert(from.ip()) {
                                    warn!("audio from unconfigured source {} — set this exact IP as a peer addr to receive it", from.ip());
                                }
                            }
                        },
                        Err(e) => warn!("recv error: {e}"),
                    }
                }

                _ = drain_tick.tick() => {
                    let mut out_block = vec![0f32; out_n_samples];
                    let mut any = false;

                    for (ri, route) in recv_routes.iter().enumerate() {
                        let jb = &mut jitters[ri];
                        if !jb.primed {
                            continue;
                        }
                        any = true;
                        if let Some((samples, src_ch)) = jb.drain_next() {
                            let src_ch = src_ch as usize;
                            for &(peer_ch, out_ch) in &route.recv {
                                let pc = peer_ch as usize;
                                let oc = out_ch as usize;
                                if pc >= src_ch || oc >= out_ch_usize {
                                    continue;
                                }
                                for f in 0..frames_usize {
                                    out_block[f * out_ch_usize + oc] += samples[f * src_ch + pc];
                                }
                            }
                        }
                    }

                    // Nothing primed yet — hold off so playout starts cleanly.
                    if !any {
                        continue;
                    }
                    let written = play_prod.push_slice(&out_block);
                    if written < out_block.len() {
                        warn!("playout ring full — dropped {} samples", out_block.len() - written);
                    }
                }
            }
        }
    });

    // --- Stats task ---------------------------------------------------------
    // Once a second, report packet counts so streaming problems are visible:
    // sent stuck at 0 → not capturing; recv ok 0 with unknown > 0 → peer IP
    // mismatch; recv all 0 → peer isn't sending / not reaching us.

    let stats_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        let (mut prev_sent, mut prev_ok) = (0u64, 0u64);
        loop {
            tick.tick().await;
            let s  = sent.load(Ordering::Relaxed);
            let ok = recv_ok.load(Ordering::Relaxed);
            let un = recv_unknown.load(Ordering::Relaxed);
            let bad = recv_bad.load(Ordering::Relaxed);
            let rs = resyncs.load(Ordering::Relaxed);
            info!(
                "stats — sent {s} (+{}/s) | recv ok {ok} (+{}/s), unknown-src {un}, bad {bad}, resyncs {rs}",
                s - prev_sent, ok - prev_ok
            );
            prev_sent = s;
            prev_ok = ok;
        }
    });

    tokio::signal::ctrl_c().await?;
    info!("shutting down audio stream");

    send_task.abort();
    recv_task.abort();
    stats_task.abort();

    drop(input_stream);
    drop(output_stream);

    Ok(())
}
