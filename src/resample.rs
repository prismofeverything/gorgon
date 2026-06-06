//! Adaptive resampling that removes residual clock drift between machines.
//!
//! Two sound cards with no shared word clock drift ~100 ppm apart; the jitter
//! buffer (`jitter.rs`) only *bounds* the resulting fill creep with a periodic
//! resync (an audible glitch every several minutes). `Resampler` wraps a
//! [`Playout`] and continuously resamples its output by a ratio steered by a DLL
//! (delay-locked loop) on the buffer fill, so the fill holds steady and playout
//! matches the local DAC exactly — the zita-njbridge approach (Fons Adriaensen,
//! "Using a DLL to filter time" / "Controlling adaptive resampling").
//!
//! Because the ratio stays within ~100 ppm of 1.0, a cheap DC-preserving cubic
//! (Catmull-Rom) interpolator is ample — and DC preservation is what keeps the
//! DC-coupled control-voltage signals faithful.

use crate::jitter::{Ingest, Playout};
use crate::packet::AudioPacket;

/// Ratio clamp: ±0.2%, ~20× the drift we expect, so two crystals never saturate
/// it but a bug can't pitch-shift audibly far.
const MAX_DRIFT: f64 = 0.002;
/// DLL loop bandwidth. Well below the rate at which crystal drift accumulates a
/// whole packet (so the loop averages many packet-arrival steps instead of
/// chasing each one), and far too slow for the ratio modulation to be audible.
const DLL_LOOP_HZ: f64 = 0.01;
/// Single-pole smoothing of the (packet-quantised, hence noisy) fill error
/// before it drives the loop, so per-packet jitter doesn't modulate the ratio.
const FILL_SMOOTH: f64 = 0.01;

/// A [`Playout`] plus continuous sample-rate adaptation. `produce_block()` is a
/// drop-in replacement for `Playout::next_block()`.
pub struct Resampler {
    play: Playout,
    /// Interleaved input frames not yet consumed. The cubic kernel reads four
    /// consecutive frames `x[i-1..=i+2]`; `fifo[0]` is `x[i-1]`.
    fifo: Vec<f32>,
    /// Current channel width; the window is rebuilt if a packet changes it.
    channels: usize,
    /// Fractional read position past `fifo[1]` (= `x[i]`), kept in [0, 1).
    phase: f64,
    /// Resample step: input frames consumed per output frame (~1.0). DLL output.
    ratio: f64,
    /// DLL integrator — converges to the true sender/receiver rate ratio.
    integ: f64,
    /// Smoothed fill error (frames) — the loop acts on this, not the raw signal.
    err_filt: f64,
    /// Target buffered depth in frames (the prime cushion).
    set_point: f64,
    /// Output frames produced per `produce_block()` call.
    frames_per_block: usize,
    frames_per_packet: usize,
    was_primed: bool,
    /// 2nd-order loop coefficients (proportional, integral), plant-gain scaled.
    b: f64,
    c: f64,
}

impl Resampler {
    pub fn new(prime: usize, sample_rate: u32, frames_per_packet: u8) -> Self {
        let fpp = frames_per_packet as usize;
        // Control rate: one DLL update per produced block (~750 Hz at 48k/64).
        let f_c = sample_rate as f64 / fpp as f64;
        let omega = 2.0 * std::f64::consts::PI * DLL_LOOP_HZ / f_c;
        // Critically-damped 2nd-order loop, divided by the plant gain (the buffer
        // moves `frames_per_block` frames per unit ratio per block).
        let b = std::f64::consts::SQRT_2 * omega / fpp as f64;
        let c = omega * omega / fpp as f64;
        Self {
            play: Playout::new(prime),
            fifo: Vec::new(),
            channels: 0,
            phase: 0.0,
            ratio: 1.0,
            integ: 0.0,
            err_filt: 0.0,
            set_point: (prime * fpp) as f64,
            frames_per_block: fpp,
            frames_per_packet: fpp,
            was_primed: false,
            b,
            c,
        }
    }

    /// Insert a decoded packet (forwards to the inner [`Playout`]).
    pub fn insert(&mut self, pkt: AudioPacket) -> Ingest {
        self.play.insert(pkt)
    }

    /// Produce one `frames_per_block`-frame block at the local DAC rate, or
    /// `None` on underrun / not-yet-primed (same contract as
    /// `Playout::next_block()`, so it drops into the occupancy-paced drain loops
    /// unchanged). Drives the DLL once per successful call.
    pub fn produce_block(&mut self) -> Option<(Vec<f32>, u8)> {
        // On each (re)prime — first prime or a post-resync re-anchor — start the
        // resampler clean so a stale ratio can't mis-pitch the fresh buffer.
        let primed = self.play.primed();
        if primed && !self.was_primed {
            self.reset();
        }
        self.was_primed = primed;
        if !primed {
            return None;
        }

        // Need input (and the channel width) before we can measure fill.
        if !self.ensure_window() {
            return None; // underrun — freeze the DLL, let the ring drain
        }
        let buffered = self.buffered_frames();
        let out = self.render_block()?; // mid-block underrun → None, DLL frozen
        self.update_dll(buffered);
        Some((out, self.channels as u8))
    }

    /// Re-seed on (re)prime: clear the window and re-discover the ratio from 1.0.
    fn reset(&mut self) {
        self.fifo.clear();
        self.channels = 0;
        self.phase = 0.0;
        self.ratio = 1.0;
        self.integ = 0.0;
        self.err_filt = 0.0;
    }

    /// Ensure the window holds ≥4 input frames around the read position, pulling
    /// fresh blocks from the `Playout`. Returns false on underrun.
    fn ensure_window(&mut self) -> bool {
        if self.channels == 0 {
            // Fresh after a reset: seed the window and duplicate the first frame
            // as the cubic's left neighbour (`x[-1]`) so playout starts on-grid.
            let (s, ch) = match self.play.next_block() {
                Some(v) => v,
                None => return false,
            };
            self.channels = ch as usize;
            self.fifo.clear();
            self.fifo.extend_from_slice(&s[..self.channels]);
            self.fifo.extend_from_slice(&s);
        }
        while self.fifo.len() / self.channels < 4 {
            let (s, ch) = match self.play.next_block() {
                Some(v) => v,
                None => return false,
            };
            if ch as usize != self.channels {
                // Peer changed width (reconfig/restart): rebuild the window.
                self.channels = ch as usize;
                self.fifo.clear();
                self.fifo.extend_from_slice(&s[..self.channels]);
                self.fifo.extend_from_slice(&s);
                self.phase = 0.0;
            } else {
                self.fifo.extend_from_slice(&s);
            }
        }
        true
    }

    /// Buffered input frames ahead of the read position — the DLL's drift signal.
    fn buffered_frames(&self) -> f64 {
        (self.play.buffered_packets() * self.frames_per_packet) as f64
            + (self.fifo.len() / self.channels) as f64
            - self.phase
    }

    /// Interpolate `frames_per_block` output frames, advancing `phase` by `ratio`
    /// per frame and consuming input as it crosses frame boundaries.
    fn render_block(&mut self) -> Option<Vec<f32>> {
        let ch = self.channels;
        let mut out = vec![0.0f32; self.frames_per_block * ch];
        for of in 0..self.frames_per_block {
            if !self.ensure_window() {
                return None;
            }
            let t = self.phase as f32;
            for c in 0..ch {
                let a = self.fifo[c];
                let b = self.fifo[ch + c];
                let cc = self.fifo[2 * ch + c];
                let d = self.fifo[3 * ch + c];
                out[of * ch + c] = hermite(a, b, cc, d, t);
            }
            self.phase += self.ratio;
            while self.phase >= 1.0 {
                self.phase -= 1.0;
                self.fifo.drain(..ch); // advance one input frame
            }
        }
        Some(out)
    }

    /// Steer the ratio toward holding the buffer at `set_point` (PI / 2nd-order
    /// DLL) acting on the low-pass-smoothed fill error. The integrator gives zero
    /// steady-state error (ratio → the true rate); smoothing keeps per-packet
    /// quantisation noise out of the proportional term. Frozen on clamp (anti-windup).
    fn update_dll(&mut self, buffered: f64) {
        let err = buffered - self.set_point;
        self.err_filt += FILL_SMOOTH * (err - self.err_filt);
        let new_integ = self.integ + self.c * self.err_filt;
        let candidate = 1.0 + self.b * self.err_filt + new_integ;
        if candidate <= 1.0 - MAX_DRIFT || candidate >= 1.0 + MAX_DRIFT {
            // Clamp the output but DON'T commit the integrator (anti-windup).
            self.ratio = candidate.clamp(1.0 - MAX_DRIFT, 1.0 + MAX_DRIFT);
        } else {
            self.integ = new_integ;
            self.ratio = candidate;
        }
    }
}

/// 4-point cubic (Catmull-Rom) interpolation of one channel at fraction `t` in
/// [0, 1) between `b` (=`x[i]`) and `c` (=`x[i+1]`), with neighbours `a`, `d`.
/// DC-preserving: the weights sum to 1, so a constant in → that constant out.
fn hermite(a: f32, b: f32, c: f32, d: f32, t: f32) -> f32 {
    let c0 = b;
    let c1 = 0.5 * (c - a);
    let c2 = a - 2.5 * b + 2.0 * c - 0.5 * d;
    let c3 = 0.5 * (d - a) + 1.5 * (b - c);
    ((c3 * t + c2) * t + c1) * t + c0
}

#[cfg(test)]
impl Resampler {
    fn test_ratio(&self) -> f64 {
        self.ratio
    }
    fn test_buffered_packets(&self) -> usize {
        self.play.buffered_packets()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FPP: u8 = 64;
    const PRIME: usize = 8;

    fn pkt(seq: u32, channels: u8, val: f32) -> AudioPacket {
        AudioPacket {
            seq,
            sample_rate: 48_000,
            channels,
            frames: FPP,
            samples: vec![val; FPP as usize * channels as usize],
        }
    }

    // --- Kernel ---------------------------------------------------------------

    #[test]
    fn hermite_preserves_dc() {
        // A constant in must give exactly that constant out, at every phase.
        for i in 0..=100 {
            let t = i as f32 / 100.0;
            assert!((hermite(0.7, 0.7, 0.7, 0.7, t) - 0.7).abs() < 1e-6, "t={t}");
        }
    }

    #[test]
    fn hermite_exact_on_lines() {
        // Catmull-Rom reproduces a straight line exactly: points 0,1,2,3 (slope 1)
        // interpolated between the middle pair give 1 + t.
        for i in 0..=100 {
            let t = i as f32 / 100.0;
            assert!((hermite(0.0, 1.0, 2.0, 3.0, t) - (1.0 + t)).abs() < 1e-5, "t={t}");
        }
    }

    // --- Pipeline -------------------------------------------------------------

    /// Drive a resampler from a sender running `ppm` off the local rate: per
    /// output block the sender produces `(1+ppm)·frames_per_block` input frames.
    /// Returns the resampler after `seconds` of simulated runtime, plus the
    /// per-block buffered-packet count over the final second.
    fn run_drift(ppm: f64, seconds: usize, channels: u8, val: f32) -> (Vec<usize>, f64) {
        let mut r = Resampler::new(PRIME, 48_000, FPP);
        let true_ratio = 1.0 + ppm * 1e-6;
        let blocks = seconds * 750; // ~750 blocks/s at 48k / 64
        let mut sender_frames = 0.0f64;
        let mut seq = 0u32;
        let mut tail_fill = Vec::new();
        let mut ratio_sum = 0.0f64;
        let mut ratio_n = 0u64;
        for k in 0..blocks {
            sender_frames += true_ratio * FPP as f64;
            while sender_frames >= FPP as f64 {
                r.insert(pkt(seq, channels, val));
                seq = seq.wrapping_add(1);
                sender_frames -= FPP as f64;
            }
            let _ = r.produce_block();
            // Over the converged second half, track fill stability and the MEAN
            // ratio: a held buffer means mean(consumed) == mean(produced), so the
            // mean ratio is the true rate. The instantaneous ratio carries an
            // inaudible sub-cent ripple from packet-granular fill, so we average.
            if k >= blocks * 3 / 4 {
                tail_fill.push(r.test_buffered_packets());
                ratio_sum += r.test_ratio();
                ratio_n += 1;
            }
        }
        (tail_fill, ratio_sum / ratio_n as f64)
    }

    #[test]
    fn dll_converges_and_holds_fill() {
        for &ppm in &[100.0_f64, -100.0, 1500.0] {
            let (tail, mean_ratio) = run_drift(ppm, 180, 1, 0.25);
            let true_ratio = 1.0 + ppm * 1e-6;
            assert!(
                (mean_ratio - true_ratio).abs() < 15e-6,
                "ppm {ppm}: mean ratio {mean_ratio} should lock to {true_ratio}"
            );
            let (lo, hi) = (*tail.iter().min().unwrap(), *tail.iter().max().unwrap());
            assert!(hi - lo <= 4, "ppm {ppm}: fill drifted instead of holding: {lo}..{hi}");
        }
    }

    #[test]
    fn dll_saturates_without_blowing_up() {
        // Drift past the ±MAX_DRIFT clamp: the ratio pins near the limit (it
        // can't keep up), stays finite, and never runs away.
        let (_, mean_ratio) = run_drift(5000.0, 20, 1, 0.1);
        assert!(
            mean_ratio.is_finite() && mean_ratio >= 1.0 + 0.0015 && mean_ratio <= 1.0 + MAX_DRIFT + 1e-9,
            "ratio {mean_ratio} should pin near the +{MAX_DRIFT} clamp"
        );
    }

    #[test]
    fn preserves_dc_through_resampling() {
        // A DC level (a held CV) must survive resampling under drift untouched.
        let mut r = Resampler::new(PRIME, 48_000, FPP);
        let true_ratio = 1.0 + 100.0 * 1e-6;
        let mut sender_frames = 0.0f64;
        let mut seq = 0u32;
        let mut checked = 0;
        for _ in 0..(10 * 750) {
            sender_frames += true_ratio * FPP as f64;
            while sender_frames >= FPP as f64 {
                r.insert(pkt(seq, 2, -0.3));
                seq += 1;
                sender_frames -= FPP as f64;
            }
            if let Some((block, ch)) = r.produce_block() {
                assert_eq!(ch, 2);
                for &s in &block {
                    assert!((s - -0.3).abs() < 1e-4, "DC not preserved: {s}");
                    checked += 1;
                }
            }
        }
        assert!(checked > 100_000, "expected sustained output, got {checked} samples");
    }

    #[test]
    fn recovers_after_resync() {
        let mut r = Resampler::new(PRIME, 48_000, FPP);
        for s in 0..PRIME as u32 {
            r.insert(pkt(s, 1, 0.1));
        }
        assert!(r.produce_block().is_some(), "should play once primed");
        // A far-ahead packet forces the jitter buffer to resync (re-prime).
        r.insert(pkt(10_000, 1, 0.2));
        for s in 10_001..(10_000 + PRIME as u32) {
            r.insert(pkt(s, 1, 0.2));
        }
        // After re-priming it produces again, at full gain (ratio reset to 1.0).
        let mut played = false;
        for _ in 0..PRIME {
            if r.produce_block().is_some() {
                played = true;
            }
        }
        assert!(played, "should resume playout after resync");
    }

    #[test]
    fn underruns_to_none() {
        let mut r = Resampler::new(PRIME, 48_000, FPP);
        for s in 0..PRIME as u32 {
            r.insert(pkt(s, 1, 0.1));
        }
        // Drain without feeding: eventually the buffer (and PLC window) run dry.
        let mut saw_none = false;
        for _ in 0..1000 {
            if r.produce_block().is_none() {
                saw_none = true;
                break;
            }
        }
        assert!(saw_none, "starved resampler must eventually return None");
    }
}
