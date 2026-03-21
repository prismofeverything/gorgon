/// Slot-indexed jitter buffer.
///
/// Packets are stored at `seq % SLOTS` so out-of-order arrivals land
/// in the right place without sorting.  The drain side advances the
/// read pointer at the packet rate regardless of whether a packet
/// arrived (missing packets → silence).
///
/// Lives entirely inside the receive task — no locking needed.

use crate::packet::AudioPacket;

pub const SLOTS: usize = 64; // must be a power of two
pub const PRIME_PACKETS: usize = 8; // packets to buffer before starting playout

pub struct JitterBuffer {
    slots:     Vec<Option<Vec<f32>>>,
    read_seq:  u32,
    filled:    usize,
    pub primed: bool,
}

impl JitterBuffer {
    pub fn new() -> Self {
        Self {
            slots:    (0..SLOTS).map(|_| None).collect(),
            read_seq: 0,
            filled:   0,
            primed:   false,
        }
    }

    /// Insert an incoming packet.  Drops packets that are too old or
    /// too far ahead to fit in the window.
    pub fn insert(&mut self, pkt: AudioPacket) {
        // On the very first packet, anchor the read head.
        if self.filled == 0 && !self.primed {
            self.read_seq = pkt.seq;
        }

        let distance = pkt.seq.wrapping_sub(self.read_seq);
        if distance >= SLOTS as u32 {
            return; // too old (already drained) or too far ahead
        }

        let idx = pkt.seq as usize % SLOTS;
        if self.slots[idx].is_none() {
            self.filled += 1;
        }
        self.slots[idx] = Some(pkt.samples);

        if !self.primed && self.filled >= PRIME_PACKETS {
            self.primed = true;
        }
    }

    /// Drain the next packet in sequence order.
    /// Returns `Some(samples)` or `None` on loss (caller should write silence).
    /// Always advances the read pointer — call once per packet interval.
    pub fn drain_next(&mut self) -> Option<Vec<f32>> {
        let idx = self.read_seq as usize % SLOTS;
        let result = self.slots[idx].take();
        if result.is_some() && self.filled > 0 {
            self.filled -= 1;
        }
        self.read_seq = self.read_seq.wrapping_add(1);
        result
    }
}
