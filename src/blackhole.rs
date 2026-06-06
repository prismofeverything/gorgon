//! macOS BlackHole bridge.
//!
//! macOS can't mint a virtual device per peer the way PipeWire does on Linux, so
//! on the Mac gorgon binds to a single pre-installed [BlackHole] loopback device
//! and multiplexes the whole group onto its channels. This module owns that one
//! duplex stream; the per-peer/-signal channel layout is decided by the caller
//! (`remote::run_macos`) and printed as a "channel map".
//!
//! The real-time callbacks here are deliberately trivial — they only shuffle
//! full-width interleaved frames between a ring buffer and the device, exactly
//! like `audio::build_input_stream` / `build_output_stream`. All the per-channel
//! placement/extraction (the fiddly part) happens off the audio thread in the
//! event loop using the pure [`place_lane`] / [`extract_lane`] helpers below,
//! which are unit-tested.
//!
//! Feedback: BlackHole loops output channel *k* back to input channel *k*. We
//! therefore split the channels into a **write region** (gorgon writes, the
//! user's app records — the low half) and a disjoint **read region** (the user's
//! app plays, gorgon reads — the high half). Because the two regions never
//! overlap, gorgon never reads back what it just wrote.
//!
//! This file uses only portable cpal, so it compiles and type-checks on every
//! platform; it is only ever *run* on macOS (see the dispatch in `remote::run`).
//!
//! [BlackHole]: https://existential.audio/blackhole/

#![cfg_attr(not(target_os = "macos"), allow(dead_code))]

use anyhow::{bail, Result};
use cpal::{BufferSize, SampleRate, Stream, StreamConfig};
use ringbuf::{
    traits::Split,
    HeapCons, HeapProd, HeapRb,
};

use crate::audio;

/// A live duplex connection to the BlackHole device. Dropping it stops both
/// streams. The two rings carry **full-width** (`channels`-channel) interleaved
/// f32 frames; the caller places/extracts individual lanes.
pub struct Hub {
    /// Number of channels on the device (e.g. 16 for "BlackHole 16ch").
    pub channels: usize,
    /// First channel of the read region — write lanes live in `0..split`,
    /// read lanes in `split..channels`.
    pub split: usize,
    /// Push full-width frames here → they play out the device (user records them).
    pub to_device: HeapProd<f32>,
    /// Pop full-width frames here ← captured from the device (what the user plays).
    pub from_device: HeapCons<f32>,
    _out_stream: Stream,
    _in_stream: Stream,
}

impl Hub {
    /// Open `device_name` (substring match, e.g. "BlackHole") as a duplex stream
    /// at 48 kHz. `ring_frames` sizes each ring (per channel).
    pub fn open(device_name: &str, ring_frames: usize) -> Result<Hub> {
        // One device handle reused for both directions (BlackHole is duplex);
        // opening the same device twice can fail, so clone the handle.
        let dev = audio::find_input_device(Some(device_name))?;
        let max_in = audio::max_input_channels(&dev)?;
        let max_out = audio::max_output_channels(&dev)?;
        let channels = max_in.min(max_out) as usize;
        if channels < 2 {
            bail!("device '{device_name}' has only {channels} duplex channel(s)");
        }

        let config = StreamConfig {
            channels: channels as u16,
            sample_rate: SampleRate(48_000),
            buffer_size: BufferSize::Default,
        };

        let (to_prod, to_cons) = HeapRb::<f32>::new(ring_frames * channels).split();
        let (from_prod, from_cons) = HeapRb::<f32>::new(ring_frames * channels).split();

        // Trivial RT callbacks (same shape as the cpal path in audio.rs).
        let out_stream = audio::build_output_stream(&dev.clone(), &config, to_cons)?;
        let in_stream = audio::build_input_stream(&dev, &config, from_prod)?;
        cpal::traits::StreamTrait::play(&out_stream)?;
        cpal::traits::StreamTrait::play(&in_stream)?;

        Ok(Hub {
            channels,
            split: channels / 2,
            to_device: to_prod,
            from_device: from_cons,
            _out_stream: out_stream,
            _in_stream: in_stream,
        })
    }
}

/// Allocates disjoint channel lanes on the device: write lanes grow up from 0,
/// read lanes grow up from `split`. Both are bounded so they never overlap.
pub struct ChannelMap {
    channels: usize,
    split: usize,
    write_next: usize,
    read_next: usize,
}

impl ChannelMap {
    pub fn new(channels: usize, split: usize) -> Self {
        Self { channels, split, write_next: 0, read_next: split }
    }

    /// Allocate `width` channels gorgon WRITES (the user records these).
    /// Returns the starting channel offset.
    pub fn alloc_write(&mut self, width: usize) -> Result<usize> {
        let off = self.write_next;
        if off + width > self.split {
            bail!("out of BlackHole record channels: need {width} past {off}, region is 0..{}", self.split);
        }
        self.write_next += width;
        Ok(off)
    }

    /// Allocate `width` channels gorgon READS (the user plays into these).
    pub fn alloc_read(&mut self, width: usize) -> Result<usize> {
        let off = self.read_next;
        if off + width > self.channels {
            bail!("out of BlackHole playback channels: need {width} past {off}, region is {}..{}", self.split, self.channels);
        }
        self.read_next += width;
        Ok(off)
    }
}

/// Copy a `width`-channel interleaved `block` into channels `[offset, offset+width)`
/// of a `channels`-wide interleaved `frame`. Other channels are left untouched.
pub fn place_lane(frame: &mut [f32], channels: usize, offset: usize, width: usize, block: &[f32]) {
    let frames = frame.len() / channels;
    for f in 0..frames {
        for c in 0..width {
            let src = f * width + c;
            if src >= block.len() {
                break;
            }
            frame[f * channels + offset + c] = block[src];
        }
    }
}

/// Pull channels `[offset, offset+width)` out of a `channels`-wide interleaved
/// `frame` into a fresh `width`-channel interleaved block.
pub fn extract_lane(frame: &[f32], channels: usize, offset: usize, width: usize) -> Vec<f32> {
    let frames = frame.len() / channels;
    let mut out = vec![0f32; frames * width];
    for f in 0..frames {
        for c in 0..width {
            out[f * width + c] = frame[f * channels + offset + c];
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn place_and_extract_round_trip() {
        let channels = 6;
        let frames = 4;
        let mut frame = vec![0f32; channels * frames];
        // Two stereo lanes at offsets 0 and 4.
        let a: Vec<f32> = (0..frames * 2).map(|i| i as f32).collect();
        let b: Vec<f32> = (0..frames * 2).map(|i| 100.0 + i as f32).collect();
        place_lane(&mut frame, channels, 0, 2, &a);
        place_lane(&mut frame, channels, 4, 2, &b);

        assert_eq!(extract_lane(&frame, channels, 0, 2), a);
        assert_eq!(extract_lane(&frame, channels, 4, 2), b);
        // Untouched middle channels (2,3) stay zero.
        assert_eq!(extract_lane(&frame, channels, 2, 2), vec![0.0; frames * 2]);
    }

    #[test]
    fn channel_map_allocates_disjoint_regions() {
        let mut m = ChannelMap::new(16, 8);
        assert_eq!(m.alloc_write(2).unwrap(), 0); // record region
        assert_eq!(m.alloc_write(3).unwrap(), 2);
        assert_eq!(m.alloc_read(2).unwrap(), 8); // playback region
        assert_eq!(m.alloc_read(1).unwrap(), 10);
        // Regions are bounded.
        assert!(m.alloc_write(5).is_err()); // 5+5 > 8
        assert!(m.alloc_read(7).is_err()); // 11+7 > 16
    }
}
