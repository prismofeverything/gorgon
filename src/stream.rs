use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use cpal::traits::{DeviceTrait, StreamTrait};
use cpal::{BufferSize, SampleRate, StreamConfig};
use ringbuf::{HeapRb, traits::{Consumer, Observer, Producer, Split}};
use tokio::net::UdpSocket;
use tracing::{info, warn};

use crate::audio;
use crate::config::Config;
use crate::jitter::JitterBuffer;
use crate::packet::AudioPacket;

/// Number of audio frames packed into each UDP packet.
/// 64 frames @ 48 kHz = ~1.33 ms per packet.
const FRAMES_PER_PACKET: u8 = 64;

/// Ring buffer capacity in frames — covers ~85 ms at 48 kHz.
const RING_FRAMES: usize = 4096;

pub async fn run(
    cfg: &Config,
    input_device:    Option<String>,
    output_device:   Option<String>,
    in_channels_override:  Option<u16>,
    out_channels_override: Option<u16>,
) -> Result<()> {
    // --- Devices and config -------------------------------------------------

    let in_dev  = audio::find_input_device(input_device.as_deref())?;
    let out_dev = audio::find_output_device(output_device.as_deref())?;

    info!("input device:  {}", in_dev.name()?);
    info!("output device: {}", out_dev.name()?);

    // Query each device independently for its maximum channel count.
    let in_channels  = in_channels_override.unwrap_or(audio::max_input_channels(&in_dev)?);
    let out_channels = out_channels_override.unwrap_or(audio::max_output_channels(&out_dev)?);
    let sample_rate  = audio::preferred_sample_rate(&in_dev)?;

    info!(
        "audio: {} Hz | in {} ch | out {} ch | {} frames/packet",
        sample_rate, in_channels, out_channels, FRAMES_PER_PACKET
    );

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

    let peers: Vec<SocketAddr> = cfg
        .peers
        .iter()
        .map(|p| SocketAddr::new(p.addr.ip(), cfg.stream_port))
        .collect();

    info!(
        "stream peers: {}",
        peers.iter().map(|a| a.to_string()).collect::<Vec<_>>().join(", ")
    );

    // --- Send task ----------------------------------------------------------

    let send_socket  = Arc::clone(&socket);
    let send_peers   = peers.clone();
    let frames       = FRAMES_PER_PACKET;
    let in_n_samples = frames as usize * in_channels as usize;

    let send_task = tokio::spawn(async move {
        let mut cap_cons  = cap_cons;
        let mut seq: u32  = 0;
        let mut frame_buf = vec![0f32; in_n_samples];

        loop {
            while cap_cons.occupied_len() < in_n_samples {
                tokio::task::yield_now().await;
            }

            let popped = cap_cons.pop_slice(&mut frame_buf);
            if popped < in_n_samples {
                continue;
            }

            let pkt = AudioPacket {
                seq,
                sample_rate: sample_rate as u16,
                channels: in_channels as u8,
                frames,
                samples: frame_buf.clone(),
            };
            let encoded = pkt.encode();
            seq = seq.wrapping_add(1);

            for peer in &send_peers {
                if let Err(e) = send_socket.send_to(&encoded, peer).await {
                    warn!("send → {peer}: {e}");
                }
            }
        }
    });

    // --- Receive task -------------------------------------------------------
    //
    // Receives packets from peers, inserts into the jitter buffer, and drains
    // it into the playout ring on a timer.  Channel count mismatch between
    // sender and local output is handled by adapt_channels().

    let recv_socket    = Arc::clone(&socket);
    let packet_dur_us  = (frames as u64 * 1_000_000) / sample_rate as u64;
    let out_n_samples  = frames as usize * out_channels as usize;
    let silence        = vec![0f32; out_n_samples];
    // Allocate the recv buffer large enough for the biggest plausible packet.
    let max_packet_len = AudioPacket::wire_len(frames, 32);

    let recv_task = tokio::spawn(async move {
        let mut play_prod  = play_prod;
        let mut jitter     = JitterBuffer::new();
        let mut recv_buf   = vec![0u8; max_packet_len];
        let mut drain_tick = tokio::time::interval(Duration::from_micros(packet_dur_us));
        drain_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                result = recv_socket.recv_from(&mut recv_buf) => {
                    match result {
                        Ok((len, _from)) => {
                            if let Some(pkt) = AudioPacket::decode(&recv_buf[..len]) {
                                jitter.insert(pkt);
                            }
                        }
                        Err(e) => warn!("recv error: {e}"),
                    }
                }

                _ = drain_tick.tick() => {
                    if !jitter.primed {
                        continue;
                    }
                    let data: Vec<f32> = match jitter.drain_next() {
                        Some((samples, src_ch)) => {
                            adapt_channels(&samples, src_ch as usize, out_channels as usize, frames as usize)
                        }
                        None => silence.clone(),
                    };
                    let written = play_prod.push_slice(&data);
                    if written < data.len() {
                        warn!("playout ring full — dropped {} samples", data.len() - written);
                    }
                }
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    info!("shutting down audio stream");

    send_task.abort();
    recv_task.abort();

    drop(input_stream);
    drop(output_stream);

    Ok(())
}

/// Adapt interleaved PCM from `src_ch` channels to `dst_ch` channels.
/// Extra source channels are dropped; missing destination channels are silenced.
fn adapt_channels(src: &[f32], src_ch: usize, dst_ch: usize, frames: usize) -> Vec<f32> {
    if src_ch == dst_ch {
        return src.to_vec();
    }
    let mut out = Vec::with_capacity(frames * dst_ch);
    for frame in 0..frames {
        for ch in 0..dst_ch {
            out.push(if ch < src_ch { src[frame * src_ch + ch] } else { 0.0 });
        }
    }
    out
}
