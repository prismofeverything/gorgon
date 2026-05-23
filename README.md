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

Each machine keeps its own config — copy the template and edit it locally:

```bash
cp config.example.toml config.toml
```

`config.toml` is gitignored, so the two ends never conflict in git (only the `config.example.toml` template is tracked). A minimal config:

```toml
bind_port   = 9000   # OSC port
stream_port = 9001   # audio stream port

[audio]
input_device  = "ES9"   # substring of `gorgon stream --list-devices`
output_device = "ES9"

[[peers]]
name = "alice"
addr = "100.x.x.x:9000"   # peer's Tailscale IP + their bind_port
```

`stream_port` must match on every machine (audio is sent to the peer at *your* `stream_port`). The port in a peer's `addr` is its `bind_port`, used only for OSC. Add one `[[peers]]` block per participant. Find a machine's Tailscale IP with `tailscale ip -4`; use the numeric `100.x` address (a MagicDNS hostname won't parse).

### Channel routing

By default every input channel is sent to each peer and incoming channels map positionally onto your outputs (channel *i* → output *i*), with extra channels dropped. To control the mapping, add a routing matrix per peer:

```toml
[[peers]]
name = "alice"
addr = "100.x.x.x:9000"
send = [0, 1, 2, 3]                    # local INPUT indices to transmit, IN ORDER
recv = [[0, 8], [1, 9], [2, 10]]       # packet channel a -> local OUTPUT index b
```

`send` picks which of your inputs go out and in what order — `send[0]` becomes packet channel 0, `send[1]` becomes packet channel 1, and so on. `recv` is a two-sided contract: `a` is a **position in the peer's `send` list** (not their physical jack), and `b` is your local output index. Several incoming channels may sum into one output. Each peer gets an independent jitter buffer keyed by source IP, so multiple peers mix on your outputs.

#### ES-9 channel indices

The ES-9 presents **16 in / 16 out** over USB, and the modular jacks are not channels 1–8:

| | 0-based index |
|---|---|
| Input jack *N* (1–14) | *N* − 1 (so jack 1 = 0) |
| 1/8" output jack *N* (1–8) | *N* + 7 (so jack 1 = 8) |

Output indices 0–7 are Main 1/4" L/R, headphone, S/PDIF, and ES-5 — **set `output_channels = 16`** or the modular outputs are unreachable. (This is the default map; the ES-9 config tool can reassign it.)

## Usage

### Audio streaming

Stream raw PCM audio directly between ES-9 interfaces:

```bash
# List available audio devices
gorgon stream --list-devices

# Stream using default audio devices
gorgon stream

# Stream using a specific device (substring match; ES-9 enumerates as "ES9")
gorgon stream --input-device ES9 --output-device ES9
```

A loose substring like `ES9` picks the `plughw:` device automatically (it converts to the f32/48 kHz format gorgon uses; the bare `hw:` device rejects it). Pass a full ALSA name to force a specific device. Device names and channel counts can also live in the `[audio]` config section; CLI flags override them.

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
