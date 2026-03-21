/// Binary packet format for raw PCM audio transport.
///
/// Wire layout (all integers big-endian, samples little-endian f32):
///   [0..4]   magic:       u32  = 0x474F5244 ("GORD")
///   [4..8]   seq:         u32  wrapping sequence number
///   [8..10]  sample_rate: u16  e.g. 48000
///   [10]     channels:    u8
///   [11]     frames:      u8   frames per packet
///   [12..]   samples:     [f32 LE]  len = frames * channels

pub const MAGIC: u32 = 0x474F5244;
pub const HEADER_LEN: usize = 12;

pub struct AudioPacket {
    pub seq:         u32,
    pub sample_rate: u16,
    pub channels:    u8,
    pub frames:      u8,
    pub samples:     Vec<f32>,
}

impl AudioPacket {
    pub fn encode(&self) -> Vec<u8> {
        let n = self.frames as usize * self.channels as usize;
        let mut buf = Vec::with_capacity(HEADER_LEN + n * 4);
        buf.extend_from_slice(&MAGIC.to_be_bytes());
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.extend_from_slice(&self.sample_rate.to_be_bytes());
        buf.push(self.channels);
        buf.push(self.frames);
        for &s in &self.samples {
            buf.extend_from_slice(&s.to_le_bytes());
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        if buf.len() < HEADER_LEN {
            return None;
        }
        let magic = u32::from_be_bytes(buf[0..4].try_into().ok()?);
        if magic != MAGIC {
            return None;
        }
        let seq         = u32::from_be_bytes(buf[4..8].try_into().ok()?);
        let sample_rate = u16::from_be_bytes(buf[8..10].try_into().ok()?);
        let channels    = buf[10];
        let frames      = buf[11];
        let n           = frames as usize * channels as usize;
        let expected    = HEADER_LEN + n * 4;
        if buf.len() < expected {
            return None;
        }
        let mut samples = Vec::with_capacity(n);
        let mut off = HEADER_LEN;
        for _ in 0..n {
            samples.push(f32::from_le_bytes(buf[off..off + 4].try_into().ok()?));
            off += 4;
        }
        Some(Self { seq, sample_rate, channels, frames, samples })
    }

    pub fn wire_len(frames: u8, channels: u8) -> usize {
        HEADER_LEN + frames as usize * channels as usize * 4
    }
}
