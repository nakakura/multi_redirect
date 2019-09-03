use std::net::SocketAddr;

use bytes::Bytes;
use futures::*;
use futures::future::*;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;
use tokio::codec::BytesCodec;
use tokio::net::*;

mod error;

const MAX_DATAGRAM_SIZE: usize = 65_507;

pub fn recv(port: u16, tx: mpsc::Sender<Vec<u8>>) -> impl Future<Item=(), Error=error::ErrorEnum> {
    let local_addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    let socket = UdpSocket::bind(&local_addr).unwrap();
    loop_fn((socket, tx), |(socket, tx)| {
        socket.recv_dgram(vec![0u8; MAX_DATAGRAM_SIZE])
            .map(|(sock, data, len, _from_ip)| {
                let x = &data[0..len];
                let tx = tx.send(x.to_vec()).wait().unwrap();
                (sock, tx)
            })
            .then(|val| {
                match val {
                    Ok(tuple) => Ok(Loop::Continue(tuple)),
                    Err(_) => Ok(Loop::Break(()))
                }
            })
    })
}

#[derive(Debug)]
enum SenderInformation {
    Addr(SocketAddr),
    Binary(Bytes),
}

fn main() {
    let closure = |mut vec: Vec<SocketAddr>| {
        move |info: SenderInformation| {
            match info {
                SenderInformation::Addr(addr) => {
                    vec.push(addr);
                    None
                },
                SenderInformation::Binary(bytes) => {
                    Some(
                        stream::iter_ok::<_, ()>(vec.clone())
                            .map(move |addr| (bytes.clone(), addr))
                    )
                },
            }
        }
    };

    // 転送先アドレスを与えるためのstream
    let (tx_add_target_addr, rx_add_target_addr) = mpsc::channel::<SocketAddr>(100);
    let rx_add_target_addr = rx_add_target_addr.map(|addr| SenderInformation::Addr(addr));

    // socketの準備
    let sock = UdpSocket::bind(&("0.0.0.0:10000".parse().unwrap())).unwrap();
    let (socket_sink, socket_stream) = UdpFramed::new(sock, BytesCodec::new()).split();

    // 受信したパケットを転送先と合わせて送信可能なstreamにする
    let rx = socket_stream
        .map(|(msg, _addr)| SenderInformation::Binary(msg.freeze()))
        .map_err(|e| panic!(e))
        .select(rx_add_target_addr)
        .filter_map(closure(vec!()))
        .flatten()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "match error type"));

    // 転送する
    let x = socket_sink
        .send_all(rx)
        .map(|_| ())
        .map_err(|e| panic!(e));

    // 転送先アドレスを与える
    std::thread::spawn(||{
        std::thread::sleep_ms(3000);
        let tx = tx_add_target_addr.send("127.0.0.1:8000".parse().unwrap()).wait().unwrap();
        std::thread::sleep_ms(3000);
        let _ = tx.send("127.0.0.1:8001".parse().unwrap()).wait();
    });

    tokio::run(x);
}
