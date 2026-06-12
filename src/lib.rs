//! gorgon as a **library** — its realtime audio / CV / network modules exposed so
//! other crates (notably prism's `prism-audio`) can reuse the same seam the
//! `gorgon` binary is built on, with no drift between the two.
//!
//! The `gorgon` binary (`src/main.rs`) is a thin CLI shell over exactly these
//! modules. prism folds gorgon in this way (a library; the binary stays a thin
//! shell) per `prism/docs/synthesis-bigraphs.md` §IV: `prism-audio` creates the
//! lock-free `ringbuf` pairs and hands the producer/consumer to
//! [`audio::build_input_stream`] / [`audio::build_output_stream`], while a prism
//! engine driver fills/drains the other ends — the device boundary as a
//! pull-driven ring.

pub mod audio;
pub mod blackhole;
pub mod config;
pub mod jitter;
pub mod network;
pub mod osc_msg;
pub mod packet;
pub mod remote;
pub mod resample;
pub mod stream;
pub mod transport;
pub mod vdev;
