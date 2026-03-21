use anyhow::{bail, Result};
use cpal::{
    traits::{DeviceTrait, HostTrait},
    Device, Stream, StreamConfig,
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

pub fn list_devices() -> Result<()> {
    let host = cpal::default_host();
    println!("Input devices:");
    for d in host.input_devices()? {
        println!("  {}", d.name().unwrap_or_else(|_| "<unknown>".into()));
    }
    println!("Output devices:");
    for d in host.output_devices()? {
        println!("  {}", d.name().unwrap_or_else(|_| "<unknown>".into()));
    }
    Ok(())
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
