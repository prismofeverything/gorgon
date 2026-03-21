use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::UdpSocket;
use rosc::{OscPacket, decoder, encoder};
use tracing::{debug, error, warn};

const MAX_UDP: usize = 65507;

/// Broadcast an OSC packet to every peer. Fire-and-forget: UDP, no retry.
pub async fn broadcast(socket: &UdpSocket, peers: &[SocketAddr], packet: &OscPacket) {
    let bytes = match encoder::encode(packet) {
        Ok(b) => b,
        Err(e) => { error!("OSC encode error: {e}"); return; }
    };
    for peer in peers {
        if let Err(e) = socket.send_to(&bytes, peer).await {
            warn!("send to {peer} failed: {e}");
        } else {
            debug!("→ {peer}  {}", crate::osc_msg::describe(packet));
        }
    }
}

/// Receive loop — runs forever, calls `on_packet` for every valid incoming message.
pub async fn receive_loop(
    socket: Arc<UdpSocket>,
    on_packet: impl Fn(SocketAddr, OscPacket) + Send + 'static,
) {
    let mut buf = vec![0u8; MAX_UDP];
    loop {
        match socket.recv_from(&mut buf).await {
            Err(e) => { error!("recv error: {e}"); }
            Ok((len, from)) => {
                match decoder::decode_udp(&buf[..len]) {
                    Ok((_, packet)) => {
                        debug!("← {from}  {}", crate::osc_msg::describe(&packet));
                        on_packet(from, packet);
                    }
                    Err(e) => warn!("bad OSC from {from}: {e}"),
                }
            }
        }
    }
}
