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

/// Ring buffer capacity in samples (per ring).  Covers ~85 ms at 48 kHz stereo.
const RING_SAMPLES: usize = 4096 * 2;

pub async fn run(
    cfg: &Config,
    input_device:  Option<String>,
    output_device: Option<String>,
) -> Result<()> {
    // --- Devices and config -------------------------------------------------

    let in_dev  = audio::find_input_device(input_device.as_deref())?;
    let out_dev = audio::find_output_device(output_device.as_deref())?;

    info!("input device:  {}", in_dev.name()?);
    info!("output device: {}", out_dev.name()?);

    // Use the input device's default config as the canonical sample rate /
    // channel count so we don't have to negotiate.
    let supported = in_dev.default_input_config()?;
    let channels    = supported.channels();
    let sample_rate = supported.sample_rate().0;

    let stream_cfg = StreamConfig {
        channels,
        sample_rate: SampleRate(sample_rate),
        buffer_size: BufferSize::Default,
    };

    info!("audio: {} Hz, {} ch, {} frames/packet", sample_rate, channels, FRAMES_PER_PACKET);

    // --- Ring buffers -------------------------------------------------------
    //
    // capture ring:  audio input callback → send task
    // playout ring:  receive task          → audio output callback

    let (cap_prod, cap_cons)   = HeapRb::<f32>::new(RING_SAMPLES).split();
    let (play_prod, play_cons) = HeapRb::<f32>::new(RING_SAMPLES).split();

    // --- Audio streams ------------------------------------------------------

    let input_stream  = audio::build_input_stream(&in_dev,  &stream_cfg, cap_prod)?;
    let output_stream = audio::build_output_stream(&out_dev, &stream_cfg, play_cons)?;

    input_stream.play()?;
    output_stream.play()?;

    // --- Network ------------------------------------------------------------

    let bind_addr = SocketAddr::from(([0, 0, 0, 0], cfg.stream_port));
    let socket    = Arc::new(UdpSocket::bind(bind_addr).await?);
    info!("stream socket bound to {bind_addr}");

    // Peers use the same IP as in config but on stream_port.
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
    //
    // Waits until a full packet's worth of samples is in the capture ring,
    // then encodes and sends to all peers.

    let send_socket = Arc::clone(&socket);
    let send_peers  = peers.clone();
    let frames      = FRAMES_PER_PACKET;
    let n_samples   = frames as usize * channels as usize;

    let send_task = tokio::spawn(async move {
        let mut cap_cons   = cap_cons;
        let mut seq: u32   = 0;
        let mut frame_buf  = vec![0f32; n_samples];

        loop {
            // Yield until enough samples are available.
            while cap_cons.occupied_len() < n_samples {
                tokio::task::yield_now().await;
            }

            let popped = cap_cons.pop_slice(&mut frame_buf);
            if popped < n_samples {
                continue; // shouldn't happen after the check above
            }

            let pkt = AudioPacket {
                seq,
                sample_rate: sample_rate as u16,
                channels: channels as u8,
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
    // Receives packets from peers and feeds the jitter buffer.
    // A timer fires every packet-duration to drain the jitter buffer
    // into the playout ring regardless of network arrival timing.

    let recv_socket   = Arc::clone(&socket);
    let packet_dur_us = (frames as u64 * 1_000_000) / sample_rate as u64;
    let silence       = vec![0f32; n_samples];

    let recv_task = tokio::spawn(async move {
        let mut play_prod  = play_prod;
        let mut jitter     = JitterBuffer::new();
        let mut recv_buf   = vec![0u8; AudioPacket::wire_len(frames, channels as u8) + 64];
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
                    let samples = jitter.drain_next();
                    let data    = samples.as_deref().unwrap_or(&silence);
                    let written = play_prod.push_slice(data);
                    if written < data.len() {
                        warn!("playout ring full — dropped {} samples", data.len() - written);
                    }
                }
            }
        }
    });

    // Wait for Ctrl-C then clean up.
    tokio::signal::ctrl_c().await?;
    info!("shutting down audio stream");

    send_task.abort();
    recv_task.abort();

    // Drop the cpal streams explicitly to stop audio.
    drop(input_stream);
    drop(output_stream);

    Ok(())
}
