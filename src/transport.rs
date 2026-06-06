//! Audio-transport helpers shared by the point-to-point `stream` command and
//! the group `remote` command: turning a captured block into a wire packet,
//! and feeding decoded packets into a jitter buffer with prime/resync tracking.

use crate::jitter::JitterBuffer;
use crate::packet::AudioPacket;

/// Re-interleave a captured block into a packet carrying only `channels` (the
/// chosen local input indices), in order: `channels[0]` becomes packet channel
/// 0, and so on. `block` is `frames * in_channels` interleaved f32.
pub fn packetize(
    block: &[f32],
    in_channels: usize,
    channels: &[u16],
    frames: u8,
    seq: u32,
    sample_rate: u16,
) -> AudioPacket {
    let nframes = frames as usize;
    let mut samples = Vec::with_capacity(nframes * channels.len());
    for f in 0..nframes {
        let base = f * in_channels;
        for &ch in channels {
            samples.push(block[base + ch as usize]);
        }
    }
    AudioPacket {
        seq,
        sample_rate,
        channels: channels.len() as u8,
        frames,
        samples,
    }
}

/// What happened when a packet was inserted into a jitter buffer — surfaced so
/// the caller can bump its own counters and log the prime transition.
pub struct IngestOutcome {
    /// The buffer resynced to this packet (a stall, loss burst, or peer restart).
    pub resynced: bool,
    /// This packet pushed the buffer over the prime threshold for the first time.
    pub newly_primed: bool,
}

/// Insert a decoded packet into a jitter buffer, reporting the resync and
/// prime-transition events the caller cares about.
pub fn ingest(jb: &mut JitterBuffer, pkt: AudioPacket) -> IngestOutcome {
    let was_primed = jb.primed;
    let resynced = jb.insert(pkt);
    IngestOutcome {
        resynced,
        newly_primed: !was_primed && jb.primed,
    }
}

/// Add one drained packet's routed channels into an interleaved output block.
/// `recv_pairs` maps `(packet_channel, local_output_channel)`; several pairs may
/// target the same output, in which case they sum (so multiple peers mix onto
/// one physical output). Out-of-range channels are skipped.
pub fn sum_into_output(
    out_block: &mut [f32],
    samples: &[f32],
    src_channels: usize,
    out_channels: usize,
    recv_pairs: &[(u16, u16)],
    frames: usize,
) {
    for &(pc, oc) in recv_pairs {
        let (pc, oc) = (pc as usize, oc as usize);
        if pc >= src_channels || oc >= out_channels {
            continue;
        }
        for f in 0..frames {
            out_block[f * out_channels + oc] += samples[f * src_channels + pc];
        }
    }
}

/// Default playout buffer depth (ms) when `audio.jitter_ms` is unset. Shared by
/// the point-to-point `stream` path and the group `remote` path so both honor
/// the same knob and default.
pub const DEFAULT_JITTER_MS: u16 = 40;

/// How many `frames_per_packet`-sized packets make up `jitter_ms` of audio at
/// `sample_rate` — a jitter buffer's prime depth (the playout latency cushion).
/// `JitterBuffer::new` clamps this to its own window, so large values are safe;
/// a 5 ms floor keeps at least a little cushion even if misconfigured.
pub fn prime_packets(jitter_ms: Option<u16>, sample_rate: u32, frames_per_packet: u8) -> usize {
    let ms = jitter_ms.unwrap_or(DEFAULT_JITTER_MS).max(5) as usize;
    let target_frames = ms * sample_rate as usize / 1000;
    (target_frames / frames_per_packet as usize).max(1)
}
