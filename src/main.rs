use std::io;
use std::net::SocketAddr;

use bincode;
use bytes::*;
use futures::*;
use futures::future::*;
use futures::sink::Sink;
use futures::stream::Stream;
use futures::sync::mpsc;
use tokio::codec::*;
use tokio::net::*;
use failure::_core::time::Duration;
use serde::{Deserialize, Serialize};
use std::time::{Instant, SystemTime};

mod error;

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct DataWithTime {
    pub time: SystemTime,
    pub data: Vec<u8>,
}

impl DataWithTime {
    pub fn new(bytes: Bytes) -> Self {
        DataWithTime {
            time: SystemTime::now(),
            data: bytes.to_vec()
        }
    }
}

pub struct MyCodec;

impl MyCodec {
    pub fn new() -> Self {
        MyCodec {}
    }
}

impl Decoder for MyCodec {
    type Item = DataWithTime;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<Self::Item>> {
        println!("mycodec decode");
        let decoded: Result<Self::Item, _> = bincode::deserialize(&buf[..]);
        println!("mycodec decode {:?}", decoded);
        match decoded {
            Ok(d) => Ok(Some(d)),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, "bincode decode error"))
        }
    }
}

impl Encoder for MyCodec {
    type Item = DataWithTime;
    type Error = io::Error;

    fn encode(&mut self, res: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        println!("mycodec encode");
        let data = bincode::serialize(&res);
        match data {
            Ok(data) => {
                buf.extend(data);
                Ok(())
            },
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other, "bincode encode error"))
        }
    }
}


#[derive(Debug)]
enum SenderInformation {
    Addr(SocketAddr),
    Binary(DataWithTime),
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

    let recv_from_user_program_and_spread = {
        // 転送先アドレスを与えるためのstream
        let (tx_add_target_addr, rx_add_target_addr) = mpsc::channel::<SocketAddr>(100);
        let rx_add_target_addr = rx_add_target_addr.map(|addr| SenderInformation::Addr(addr));

        // socketの準備
        let sender_sock = UdpSocket::bind(&("0.0.0.0:0".parse().unwrap())).unwrap();
        let (socket_sink, _) = UdpFramed::new(sender_sock, MyCodec::new()).split();
        let receiver_sock = UdpSocket::bind(&("0.0.0.0:10000".parse().unwrap())).unwrap();
        let (_, socket_stream) = UdpFramed::new(receiver_sock, BytesCodec::new()).split();

        // 受信したパケットを転送先と合わせて送信可能なstreamにする
        let rx = socket_stream
            .map(|(msg, _addr)| SenderInformation::Binary(DataWithTime::new(msg.freeze())))
            .map_err(|e| panic!(e))
            .select(rx_add_target_addr)
            .filter_map(closure(vec!()))
            .flatten()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "match error type"));

        // 転送先アドレスを与える
        std::thread::spawn(|| {
            std::thread::sleep_ms(3000);
            let tx = tx_add_target_addr.send("127.0.0.1:8000".parse().unwrap()).wait().unwrap();
            std::thread::sleep_ms(3000);
            let _ = tx.send("127.0.0.1:8001".parse().unwrap()).wait();
        });
        // 転送する
        socket_sink
            .send_all(rx)
            .map(|_| ())
            .map_err(|e| panic!(e))
    };

    let collect_packets_and_redirect_to_userprogram = {
        let sock1 = UdpSocket::bind(&("0.0.0.0:8000".parse().unwrap())).unwrap();
        let (sender, socket_stream1) = UdpFramed::new(sock1, MyCodec::new()).split();

        let sock2 = UdpSocket::bind(&("0.0.0.0:8001".parse().unwrap())).unwrap();
        let (_, socket_stream2) = UdpFramed::new(sock2, MyCodec::new()).split();

        let sock3 = UdpSocket::bind(&("0.0.0.0:8002".parse().unwrap())).unwrap();
        let (_, socket_stream3) = UdpFramed::new(sock3, MyCodec::new()).split();

        let sender_sock = UdpSocket::bind(&("0.0.0.0:0".parse().unwrap())).unwrap();
        let (socket_sink, _) = UdpFramed::new(sender_sock, BytesCodec::new()).split();

        let addr: SocketAddr = "127.0.0.1:9000".parse().unwrap();

        let recv_stream = socket_stream1
            .map(move |(data, _addr)| {
                (data.data.into(), addr)
            })
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "match error type"));

        socket_sink
            .send_all(recv_stream)
            .map(|_| ())
            .map_err(|e| panic!(e))
    };

    let interval = tokio::timer::Interval::new_interval(Duration::from_millis(10));
    let interval = interval
        .for_each(|x| {
            println!("x{:?}", x);
            Ok(())
        })
        .map_err(|e| panic!(e));

    tokio::run(recv_from_user_program_and_spread.join(interval).join(collect_packets_and_redirect_to_userprogram).map(|_| ()).map_err(|e| panic!(e)));
}
