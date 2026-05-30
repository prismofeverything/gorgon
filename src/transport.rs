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
