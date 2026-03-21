use anyhow::{bail, Result};
use libc;
use cpal::{
    traits::{DeviceTrait, HostTrait},
    Device, SampleRate, Stream, StreamConfig,
};
use ringbuf::traits::{Consumer, Producer};
use tracing::warn;

pub fn find_input_device(name: Option<&str>) -> Result<Device> {
    let host = cpal::default_host();
    match name {
        None => host
            .default_input_device()
            .ok_or_else(|| anyhow::anyhow!("no default input device")),
        Some(n) => {
            for d in host.input_devices()? {
                if d.name().map(|s| s.contains(n)).unwrap_or(false) {
                    return Ok(d);
                }
            }
            bail!("no input device matching '{n}'")
        }
    }
}

pub fn find_output_device(name: Option<&str>) -> Result<Device> {
    let host = cpal::default_host();
    match name {
        None => host
            .default_output_device()
            .ok_or_else(|| anyhow::anyhow!("no default output device")),
        Some(n) => {
            for d in host.output_devices()? {
                if d.name().map(|s| s.contains(n)).unwrap_or(false) {
                    return Ok(d);
                }
            }
            bail!("no output device matching '{n}'")
        }
    }
}

/// Highest channel count supported by the device for input.
pub fn max_input_channels(device: &Device) -> Result<u16> {
    device
        .supported_input_configs()?
        .map(|r| r.channels())
        .max()
        .ok_or_else(|| anyhow::anyhow!("no supported input configs"))
}

/// Highest channel count supported by the device for output.
pub fn max_output_channels(device: &Device) -> Result<u16> {
    device
        .supported_output_configs()?
        .map(|r| r.channels())
        .max()
        .ok_or_else(|| anyhow::anyhow!("no supported output configs"))
}

/// Preferred sample rate: 48 kHz if the device supports it, otherwise the default.
pub fn preferred_sample_rate(device: &Device) -> Result<u32> {
    const TARGET: SampleRate = SampleRate(48_000);
    for range in device.supported_input_configs()? {
        if range.min_sample_rate() <= TARGET && TARGET <= range.max_sample_rate() {
            return Ok(48_000);
        }
    }
    Ok(device.default_input_config()?.sample_rate().0)
}

pub fn list_devices() -> Result<()> {
    // ALSA writes probe errors directly to stderr via the C library.
    // Suppress that fd during enumeration and restore it after.
    let _guard = StderrSuppressor::new();

    let host = cpal::default_host();
    let mut seen = std::collections::HashSet::new();

    println!("Input:");
    for d in host.input_devices()? {
        if let Ok(name) = d.name() {
            if seen.insert(name.clone()) {
                println!("  {name}");
            }
        }
    }

    seen.clear();
    println!("Output:");
    for d in host.output_devices()? {
        if let Ok(name) = d.name() {
            if seen.insert(name.clone()) {
                println!("  {name}");
            }
        }
    }

    Ok(())
}

/// Redirects stderr (fd 2) to /dev/null for its lifetime, then restores it.
struct StderrSuppressor {
    saved_fd: libc::c_int,
}

impl StderrSuppressor {
    fn new() -> Self {
        let saved_fd = unsafe { libc::dup(2) };
        let null_fd = unsafe {
            libc::open(b"/dev/null\0".as_ptr() as *const libc::c_char, libc::O_WRONLY)
        };
        if null_fd >= 0 {
            unsafe {
                libc::dup2(null_fd, 2);
                libc::close(null_fd);
            }
        }
        Self { saved_fd }
    }
}

impl Drop for StderrSuppressor {
    fn drop(&mut self) {
        if self.saved_fd >= 0 {
            unsafe {
                libc::dup2(self.saved_fd, 2);
                libc::close(self.saved_fd);
            }
        }
    }
}

/// Build a cpal input stream that feeds captured f32 samples into `prod`.
/// The callback is lock-free — safe to use from a real-time audio thread.
pub fn build_input_stream<P>(device: &Device, config: &StreamConfig, mut prod: P) -> Result<Stream>
where
    P: Producer<Item = f32> + Send + 'static,
{
    let stream = device.build_input_stream::<f32, _, _>(
        config,
        move |data: &[f32], _: &cpal::InputCallbackInfo| {
            let pushed = prod.push_slice(data);
            if pushed < data.len() {
                warn!("capture ring full — dropped {} samples", data.len() - pushed);
            }
        },
        |e| tracing::error!("input stream error: {e}"),
        None,
    )?;
    Ok(stream)
}

/// Build a cpal output stream that drains f32 samples from `cons`.
/// Fills with silence when the ring runs dry.
pub fn build_output_stream<C>(device: &Device, config: &StreamConfig, mut cons: C) -> Result<Stream>
where
    C: Consumer<Item = f32> + Send + 'static,
{
    let stream = device.build_output_stream::<f32, _, _>(
        config,
        move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
            let popped = cons.pop_slice(data);
            data[popped..].fill(0.0); // silence on underrun
        },
        |e| tracing::error!("output stream error: {e}"),
        None,
    )?;
    Ok(stream)
}
