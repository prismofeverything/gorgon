/// Slot-indexed jitter buffer.
///
/// Packets are stored at `seq % SLOTS` so out-of-order arrivals land
/// in the right place without sorting.  The drain side advances the
/// read pointer at the packet rate regardless of whether a packet
/// arrived (missing packets → silence).
///
/// Lives entirely inside the receive task — no locking needed.

use crate::packet::AudioPacket;

pub const SLOTS: usize = 256; // must be a power of two; window ≈ 340 ms at 64-frame packets

pub struct JitterBuffer {
    slots:      Vec<Option<(Vec<f32>, u8)>>, // (samples, src_channels)
    read_seq:   u32,
    filled:     usize,
    prime:      usize, // packets to buffer before playout starts
    pub primed: bool,
}

impl JitterBuffer {
    /// `prime` is how many packets to buffer before playout begins — the
    /// playout latency cushion, in packets.
    pub fn new(prime: usize) -> Self {
        let prime = prime.clamp(1, SLOTS / 2);
        Self {
            slots:    (0..SLOTS).map(|_| None::<(Vec<f32>, u8)>).collect(),
            read_seq: 0,
            filled:   0,
            prime,
            primed:   false,
        }
    }

    /// Insert an incoming packet. Recent-but-late packets are dropped; a packet
    /// that lands far outside the window in either direction means the streams
    /// have desynced (a stall, loss burst, or the peer restarting) — resync to
    /// it and re-prime rather than wedging forever. Returns `true` on resync.
    pub fn insert(&mut self, pkt: AudioPacket) -> bool {
        // On the very first packet, anchor the read head.
        if self.filled == 0 && !self.primed {
            self.read_seq = pkt.seq;
        }

        // `ahead` is pkt.seq - read_seq in wrapping (modular) arithmetic:
        //   0..SLOTS                  -> inside the window, in the future
        //   (MAX-SLOTS)..MAX          -> a few packets late (recent reorder)
        //   anything else             -> way out of range -> desync
        let ahead = pkt.seq.wrapping_sub(self.read_seq);
        if ahead >= SLOTS as u32 {
            if ahead > u32::MAX - SLOTS as u32 {
                return false; // recently drained / reordered — just drop it
            }
            self.resync(pkt);
            return true;
        }

        let idx = pkt.seq as usize % SLOTS;
        if self.slots[idx].is_none() {
            self.filled += 1;
        }
        self.slots[idx] = Some((pkt.samples, pkt.channels));

        if !self.primed && self.filled >= self.prime {
            self.primed = true;
        }
        false
    }

    /// Discard buffered audio and re-anchor on `pkt`, then re-prime. Costs a
    /// brief (~PRIME_PACKETS) silence but recovers a stuck stream automatically.
    fn resync(&mut self, pkt: AudioPacket) {
        for slot in &mut self.slots {
            *slot = None;
        }
        self.read_seq = pkt.seq;
        let idx = pkt.seq as usize % SLOTS;
        self.slots[idx] = Some((pkt.samples, pkt.channels));
        self.filled = 1;
        self.primed = false;
    }

    /// Drain the next packet in sequence order.
    /// Returns `Some((samples, src_channels))` or `None` on loss (caller should write silence).
    /// Always advances the read pointer — call once per packet interval.
    pub fn drain_next(&mut self) -> Option<(Vec<f32>, u8)> {
        let idx = self.read_seq as usize % SLOTS;
        let result = self.slots[idx].take();
        if result.is_some() && self.filled > 0 {
            self.filled -= 1;
        }
        self.read_seq = self.read_seq.wrapping_add(1);
        result
    }
}

/// On a missing packet, repeat the last block this many times (fading) before
/// falling back to silence — masks isolated losses without obvious looping.
pub const PLC_MAX_REPEATS: u32 = 3;
/// Per-repeat amplitude decay applied during packet-loss concealment.
pub const PLC_DECAY: f32 = 0.6;

/// A jitter buffer plus packet-loss-concealment (PLC) state. On a missing packet
/// it replays the last decoded block, fading, for up to `PLC_MAX_REPEATS` packets
/// before going silent — the same concealment the point-to-point `stream` path
/// applies inline, packaged so the `remote` drain paths can share it.
pub struct Playout {
    jb: JitterBuffer,
    last: Option<(Vec<f32>, u8)>,
    miss: u32,
}

impl Playout {
    pub fn new(prime: usize) -> Self {
        Self { jb: JitterBuffer::new(prime), last: None, miss: 0 }
    }

    /// Whether the buffer has filled its prime cushion and is playing out.
    pub fn primed(&self) -> bool {
        self.jb.primed
    }

    /// Insert a decoded packet; returns `true` if this packet just primed the
    /// buffer for the first time (so the caller can log the transition).
    pub fn insert(&mut self, pkt: AudioPacket) -> bool {
        let was_primed = self.jb.primed;
        self.jb.insert(pkt);
        !was_primed && self.jb.primed
    }

    /// The next block to play this tick: the freshly drained packet (full gain),
    /// or a faded copy of the last one during concealment, or `None` when there
    /// is nothing to play (not primed, or the loss ran past the PLC window).
    /// Advances the read pointer once per call — call once per packet interval.
    pub fn next_block(&mut self) -> Option<(Vec<f32>, u8)> {
        if !self.jb.primed {
            return None;
        }
        match self.jb.drain_next() {
            Some((samples, src_ch)) => {
                self.last = Some((samples.clone(), src_ch));
                self.miss = 0;
                Some((samples, src_ch))
            }
            None => {
                // Lost/late packet: conceal by repeating the last block, fading,
                // for a few packets before falling back to silence.
                if self.miss < PLC_MAX_REPEATS {
                    if let Some((samples, src_ch)) = &self.last {
                        let gain = PLC_DECAY.powi(self.miss as i32 + 1);
                        let faded = samples.iter().map(|s| s * gain).collect();
                        let src_ch = *src_ch;
                        self.miss += 1;
                        return Some((faded, src_ch));
                    }
                }
                None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PRIME: u32 = 8;

    fn pkt(seq: u32) -> AudioPacket {
        AudioPacket { seq, sample_rate: 48_000, channels: 1, frames: 1, samples: vec![seq as f32] }
    }

    #[test]
    fn primes_after_prime_packets() {
        let mut jb = JitterBuffer::new(PRIME as usize);
        for s in 0..PRIME {
            assert!(!jb.insert(pkt(s)), "in-window insert must not resync");
        }
        assert!(jb.primed);
    }

    #[test]
    fn recent_late_packet_is_dropped_not_resynced() {
        let mut jb = JitterBuffer::new(PRIME as usize);
        for s in 0..PRIME {
            jb.insert(pkt(s));
        }
        for _ in 0..3 {
            jb.drain_next(); // advance read_seq to 3
        }
        // seq 0 is now behind the read head — stale reorder, drop without resync
        assert!(!jb.insert(pkt(0)));
    }

    #[test]
    fn large_forward_jump_resyncs_and_recovers() {
        let mut jb = JitterBuffer::new(PRIME as usize);
        for s in 0..PRIME {
            jb.insert(pkt(s));
        }
        assert!(jb.primed);

        // Sender raced far ahead (stall / restart) — the old code would wedge here.
        let jump = 10_000u32;
        assert!(jb.insert(pkt(jump)), "far-ahead packet should resync");
        assert!(!jb.primed, "resync re-primes from scratch");

        // Fill the cushion at the new sequence and confirm playout resumes in order.
        for s in (jump + 1)..(jump + PRIME) {
            jb.insert(pkt(s));
        }
        assert!(jb.primed);
        let (samples, _) = jb.drain_next().expect("resynced stream should drain");
        assert_eq!(samples[0], jump as f32, "drains from the resynced sequence");
    }

    #[test]
    fn playout_conceals_lost_packets_then_silences_then_recovers() {
        let mut p = Playout::new(PRIME as usize);
        for s in 0..PRIME {
            p.insert(pkt(s));
        }
        assert!(p.primed());

        // The primed packets play out in order at full gain.
        for s in 0..PRIME {
            let (samples, _) = p.next_block().expect("primed buffer plays out");
            assert_eq!(samples[0], s as f32);
        }

        // The following packets are missing: conceal by fading the last block
        // (value PRIME-1) for PLC_MAX_REPEATS packets, then fall silent.
        let last = (PRIME - 1) as f32;
        for r in 1..=PLC_MAX_REPEATS {
            let (samples, _) = p.next_block().expect("within PLC window → conceal");
            let expected = last * PLC_DECAY.powi(r as i32);
            assert!((samples[0] - expected).abs() < 1e-6, "faded repeat {r}");
        }
        assert!(p.next_block().is_none(), "past the PLC window → silence");

        // A fresh packet recovers full-gain playout. The read head has advanced
        // by PRIME real packets plus PLC_MAX_REPEATS + 1 concealment/silence ticks.
        let next_seq = PRIME + PLC_MAX_REPEATS + 1;
        p.insert(pkt(next_seq));
        let (samples, _) = p.next_block().expect("fresh packet plays");
        assert_eq!(samples[0], next_seq as f32);
    }
}
