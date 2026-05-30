//! Virtual audio devices.
//!
//! A virtual device presents a *remote* group member's exposed signals as a
//! real, selectable audio device on the local machine. It sits behind the same
//! ring-buffer interface the cpal path uses (`audio::build_input_stream` /
//! `build_output_stream` take a ringbuf `Producer`/`Consumer`), so the network
//! tasks never need to know whether an endpoint is a physical interface (cpal)
//! or a virtual device (PipeWire) — they just push/pop f32 samples.
//!
//!   * [`source`] — a node other apps **record from**. gorgon FEEDS it from the
//!     network (pops from the `Consumer`). Used for a peer's exposed *outputs*.
//!   * [`sink`] — a node other apps **play into**. gorgon DRAINS it to the
//!     network (pushes into the `Producer`). Used for a peer's exposed *inputs*.
//!
//! Linux: native PipeWire nodes, created in-process — no external tools, no
//! sudo, no kernel module. macOS will bind a user-installed BlackHole device
//! via cpal (not implemented yet — see the `remote` command's phasing).
//!
//! Channels are positioned `AUX0..AUXn` (not FL/FR), which keeps DC-coupled CV
//! clear of any stereo/surround down-mix heuristics. The human signal names
//! ("kick", "bass") live in gorgon's config, not in the OS — apps see AUXn.

use anyhow::Result;
use ringbuf::traits::{Consumer, Producer};

/// A live virtual device. Dropping it stops the PipeWire loop thread and
/// removes the node from the graph.
pub struct VDevice {
    #[cfg(target_os = "linux")]
    _inner: linux::PwNode,
}

/// Create a virtual SOURCE device named `name` with `channels` channels. Other
/// apps record from it; gorgon fills it by popping f32 frames from `cons`.
pub fn source<C>(name: &str, channels: u32, cons: C) -> Result<VDevice>
where
    C: Consumer<Item = f32> + Send + 'static,
{
    #[cfg(target_os = "linux")]
    {
        Ok(VDevice {
            _inner: linux::spawn_source(name.to_string(), channels, cons)?,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (name, channels, cons);
        anyhow::bail!("virtual audio devices are only implemented on Linux (PipeWire) so far");
    }
}

/// Create a virtual SINK device named `name` with `channels` channels. Other
/// apps play into it; gorgon drains it by pushing the f32 frames into `prod`.
#[cfg_attr(target_os = "linux", allow(dead_code))] // wired up in the bidirectional phase
pub fn sink<P>(name: &str, channels: u32, prod: P) -> Result<VDevice>
where
    P: Producer<Item = f32> + Send + 'static,
{
    #[cfg(target_os = "linux")]
    {
        Ok(VDevice {
            _inner: linux::spawn_sink(name.to_string(), channels, prod)?,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (name, channels, prod);
        anyhow::bail!("virtual audio devices are only implemented on Linux (PipeWire) so far");
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use anyhow::Result;
    use pipewire as pw;
    use pw::spa::param::audio::{AudioFormat, AudioInfoRaw};
    use pw::spa::pod::Pod;
    use pw::spa::utils::Direction;
    use ringbuf::traits::{Consumer, Producer};
    use std::mem::size_of;
    use std::sync::mpsc;

    /// In the loop thread's setup phase, unwrap a `Result` or report the error
    /// back to `spawn_*` (via `$ready`) and stop the thread.
    macro_rules! setup_try {
        ($ready:expr, $e:expr) => {
            match $e {
                Ok(v) => v,
                Err(err) => {
                    let _ = $ready.send(Err(format!("{err:#}")));
                    return;
                }
            }
        };
    }

    /// Owns the dedicated PipeWire main-loop thread. Dropping it asks the loop
    /// to quit and joins the thread, which tears the node out of the graph.
    pub struct PwNode {
        quit: Option<pw::channel::Sender<()>>,
        handle: Option<std::thread::JoinHandle<()>>,
    }

    impl Drop for PwNode {
        fn drop(&mut self) {
            if let Some(q) = self.quit.take() {
                let _ = q.send(());
            }
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    type Ready = mpsc::Sender<Result<(), String>>;

    /// Spawn the loop thread for a source node and block until it is connected,
    /// so PipeWire errors surface here rather than silently in the thread.
    pub fn spawn_source<C>(name: String, channels: u32, cons: C) -> Result<PwNode>
    where
        C: Consumer<Item = f32> + Send + 'static,
    {
        let (quit_tx, quit_rx) = pw::channel::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = std::thread::Builder::new()
            .name(format!("pw-src-{name}"))
            .spawn(move || run_source(name, channels, cons, quit_rx, ready_tx))?;
        finish(quit_tx, handle, ready_rx)
    }

    /// Spawn the loop thread for a sink node (see [`spawn_source`]).
    pub fn spawn_sink<P>(name: String, channels: u32, prod: P) -> Result<PwNode>
    where
        P: Producer<Item = f32> + Send + 'static,
    {
        let (quit_tx, quit_rx) = pw::channel::channel::<()>();
        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = std::thread::Builder::new()
            .name(format!("pw-sink-{name}"))
            .spawn(move || run_sink(name, channels, prod, quit_rx, ready_tx))?;
        finish(quit_tx, handle, ready_rx)
    }

    /// Wait for the loop thread's setup to succeed (or fail) and build the handle.
    fn finish(
        quit_tx: pw::channel::Sender<()>,
        handle: std::thread::JoinHandle<()>,
        ready_rx: mpsc::Receiver<Result<(), String>>,
    ) -> Result<PwNode> {
        match ready_rx.recv() {
            Ok(Ok(())) => Ok(PwNode {
                quit: Some(quit_tx),
                handle: Some(handle),
            }),
            Ok(Err(e)) => anyhow::bail!("pipewire node setup failed: {e}"),
            Err(_) => anyhow::bail!("pipewire node thread died during setup"),
        }
    }

    fn run_source<C>(
        name: String,
        channels: u32,
        cons: C,
        quit_rx: pw::channel::Receiver<()>,
        ready_tx: Ready,
    ) where
        C: Consumer<Item = f32> + Send + 'static,
    {
        pw::init();
        let mainloop = setup_try!(ready_tx, pw::main_loop::MainLoop::new(None));
        let context = setup_try!(ready_tx, pw::context::Context::new(&mainloop));
        let core = setup_try!(ready_tx, context.connect(None));
        let _quit = quit_rx.attach(mainloop.loop_(), {
            let ml = mainloop.clone();
            move |_| ml.quit()
        });

        let stream = setup_try!(
            ready_tx,
            pw::stream::Stream::new(&core, &name, node_properties(&name, "Audio/Source"))
        );

        let chans = channels as usize;
        // The consumer lives in the stream's user-data and is touched only from
        // this real-time `process` callback — no locks, like the cpal callbacks.
        let _listener = setup_try!(
            ready_tx,
            stream
                .add_local_listener_with_user_data(cons)
                .process(move |stream, cons| {
                    let Some(mut buffer) = stream.dequeue_buffer() else {
                        return;
                    };
                    let datas = buffer.datas_mut();
                    let Some(data) = datas.get_mut(0) else { return };

                    let mut size = 0usize;
                    if let Some(raw) = data.data() {
                        let usable = raw.len() - raw.len() % size_of::<f32>();
                        let floats: &mut [f32] = bytemuck::cast_slice_mut(&mut raw[..usable]);
                        let frames = floats.len() / chans;
                        let want = frames * chans;
                        let popped = cons.pop_slice(&mut floats[..want]);
                        floats[popped..want].fill(0.0); // silence on underrun
                        size = want * size_of::<f32>();
                    }
                    let chunk = data.chunk_mut();
                    *chunk.offset_mut() = 0;
                    *chunk.stride_mut() = (chans * size_of::<f32>()) as _;
                    *chunk.size_mut() = size as _;
                })
                .register()
        );

        run_connected(&stream, Direction::Output, channels, &mainloop, &ready_tx);
    }

    fn run_sink<P>(
        name: String,
        channels: u32,
        prod: P,
        quit_rx: pw::channel::Receiver<()>,
        ready_tx: Ready,
    ) where
        P: Producer<Item = f32> + Send + 'static,
    {
        pw::init();
        let mainloop = setup_try!(ready_tx, pw::main_loop::MainLoop::new(None));
        let context = setup_try!(ready_tx, pw::context::Context::new(&mainloop));
        let core = setup_try!(ready_tx, context.connect(None));
        let _quit = quit_rx.attach(mainloop.loop_(), {
            let ml = mainloop.clone();
            move |_| ml.quit()
        });

        let stream = setup_try!(
            ready_tx,
            pw::stream::Stream::new(&core, &name, node_properties(&name, "Audio/Sink"))
        );

        let _listener = setup_try!(
            ready_tx,
            stream
                .add_local_listener_with_user_data(prod)
                .process(move |stream, prod| {
                    let Some(mut buffer) = stream.dequeue_buffer() else {
                        return;
                    };
                    let datas = buffer.datas_mut();
                    let Some(data) = datas.get_mut(0) else { return };

                    let valid = data.chunk().size() as usize;
                    if let Some(raw) = data.data() {
                        let end = valid.min(raw.len());
                        let end = end - end % size_of::<f32>();
                        let floats: &[f32] = bytemuck::cast_slice(&raw[..end]);
                        let _ = prod.push_slice(floats); // drop excess on overrun
                    }
                })
                .register()
        );

        run_connected(&stream, Direction::Input, channels, &mainloop, &ready_tx);
    }

    /// Connect the stream (publishing the device), signal readiness, and run the
    /// loop until quit. Shared tail of `run_source`/`run_sink`.
    fn run_connected(
        stream: &pw::stream::Stream,
        direction: Direction,
        channels: u32,
        mainloop: &pw::main_loop::MainLoop,
        ready_tx: &Ready,
    ) {
        let mut pod = Vec::new();
        build_format_pod(channels, &mut pod);
        let pod_ref = match Pod::from_bytes(&pod) {
            Some(p) => p,
            None => {
                let _ = ready_tx.send(Err("failed to build format POD".into()));
                return;
            }
        };
        let mut params = [pod_ref];

        // No AUTOCONNECT: publish a device for apps to route to, rather than
        // latching onto the default sink/source. NO_CONVERT keeps PipeWire from
        // inserting a resampler/format converter on the link.
        let flags = pw::stream::StreamFlags::MAP_BUFFERS
            | pw::stream::StreamFlags::RT_PROCESS
            | pw::stream::StreamFlags::NO_CONVERT;
        if let Err(e) = stream.connect(direction, None, flags, &mut params) {
            let _ = ready_tx.send(Err(format!("{e:#}")));
            return;
        }

        let _ = ready_tx.send(Ok(()));
        mainloop.run(); // blocks this thread until the loop is quit on Drop
    }

    /// Node properties: identity plus the settings that keep DC-coupled CV
    /// bit-exact (forced 48 kHz f32, no channel mixing, no monitor volume).
    /// All keys are plain strings — `Properties::insert` takes `Into<Vec<u8>>`.
    fn node_properties(name: &str, media_class: &str) -> pw::properties::Properties {
        let mut p = pw::properties::Properties::new();
        p.insert("media.type", "Audio");
        p.insert("media.class", media_class);
        p.insert("node.name", name);
        p.insert("node.description", name);
        p.insert("node.virtual", "true");
        p.insert("audio.rate", "48000");
        p.insert("audio.format", "F32");
        p.insert("node.force-rate", "48000");
        p.insert("channelmix.disable", "true");
        p.insert("monitor.channel-volumes", "false");
        p
    }

    /// Serialize an `EnumFormat` POD: F32LE, 48 kHz, `channels` channels, each
    /// positioned as AUX0..AUXn so no surround/stereo logic touches the signal.
    fn build_format_pod(channels: u32, out: &mut Vec<u8>) {
        let mut info = AudioInfoRaw::new();
        info.set_format(AudioFormat::F32LE);
        info.set_rate(48_000);
        info.set_channels(channels);

        let mut pos = [pw::spa::sys::SPA_AUDIO_CHANNEL_UNKNOWN; 64];
        for (i, slot) in pos.iter_mut().enumerate().take((channels as usize).min(64)) {
            *slot = pw::spa::sys::SPA_AUDIO_CHANNEL_AUX0 + i as u32;
        }
        info.set_position(pos);

        let obj = pw::spa::pod::Object {
            type_: pw::spa::utils::SpaTypes::ObjectParamFormat.as_raw(),
            id: pw::spa::param::ParamType::EnumFormat.as_raw(),
            properties: info.into(),
        };
        *out = pw::spa::pod::serialize::PodSerializer::serialize(
            std::io::Cursor::new(Vec::new()),
            &pw::spa::pod::Value::Object(obj),
        )
        .unwrap()
        .0
        .into_inner();
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use ringbuf::{traits::Split, HeapRb};

    /// Creates a real PipeWire node, so it's `#[ignore]`d — run it by hand on a
    /// box with a PipeWire server:
    ///   `cargo test creates_a_visible_source_node -- --ignored --nocapture`
    #[test]
    #[ignore = "spawns a real PipeWire node; needs a running PipeWire server"]
    fn creates_a_visible_source_node() {
        let (_prod, cons) = HeapRb::<f32>::new(4096).split();
        // `source` only returns Ok after PipeWire accepts and publishes the node.
        let dev = source("gorgon-selftest", 2, cons).expect("source node should connect");
        std::thread::sleep(std::time::Duration::from_millis(500));

        match std::process::Command::new("pw-cli").args(["ls", "Node"]).output() {
            Ok(o) => {
                let listing = String::from_utf8_lossy(&o.stdout);
                assert!(
                    listing.contains("gorgon-selftest"),
                    "node 'gorgon-selftest' not found in `pw-cli ls Node`"
                );
                eprintln!("ok: virtual node 'gorgon-selftest' is live in the PipeWire graph");
            }
            Err(e) => eprintln!("(pw-cli unavailable: {e}) — source() returning Ok already implies connect succeeded"),
        }
        drop(dev);
    }
}
