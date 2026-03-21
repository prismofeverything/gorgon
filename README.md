# gorgon

P2P audio and CV messenger over Tailscale. Send raw PCM audio between two ES-9 interfaces, or exchange OSC control-voltage messages, with no central server.

## Requirements

- [Rust](https://rustup.rs)
- [Tailscale](https://tailscale.com) (for peer connectivity)
- `libasound2-dev` (Linux, for ALSA audio)

```bash
sudo apt-get install libasound2-dev
cargo build --release
```

## Configuration

Edit `config.toml`:

```toml
bind_port   = 9000   # OSC port
stream_port = 9001   # audio stream port

[[peers]]
name = "alice"
addr = "100.x.x.x:9000"   # peer's Tailscale IP
```

Both machines must use the same port numbers. Add one `[[peers]]` block per participant.

## Usage

### Audio streaming

Stream raw PCM audio directly between ES-9 interfaces:

```bash
# List available audio devices
gorgon stream --list-devices

# Stream using default audio devices
gorgon stream

# Stream using a specific device (substring match)
gorgon stream --input-device ES-9 --output-device ES-9
```

Both peers run `gorgon stream`. Audio is captured from the input device, packetized as raw f32 PCM, and sent over UDP to all configured peers. Incoming audio is buffered and played back on the output device.

Because the ES-9 carries DC-coupled CV signals alongside audio, no lossy codec is used — the signal is transmitted bit-for-bit.

### OSC messages

Send CV, gate, and MIDI note values to all peers:

```bash
# Listen for incoming OSC messages
gorgon listen

# Send a CV value (0.0 – 1.0) on a named channel
gorgon cv pitch 0.75

# Send a gate on or off
gorgon gate trigger on
gorgon gate trigger off

# Send a MIDI note number (0 – 127)
gorgon note bass 60
```

OSC messages use the address scheme `/cv/<channel>`, `/gate/<channel>`, `/note/<channel>` and are compatible with VCV Rack's **cvosccv** module. Point cvosccv's output at your peer's Tailscale IP on port 9000.

## Latency

Audio latency depends on network conditions and jitter buffer depth. Typical one-way latency over Tailscale between nearby machines is 20–50 ms. The jitter buffer primes with ~10 ms of packets before starting playback, trading a small fixed latency for dropout resilience.
