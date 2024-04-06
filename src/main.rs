mod net;

extern crate core;

use std::io::ErrorKind;
use std::sync::{Arc};
use tokio::net::{TcpListener, TcpStream};
use clap::Parser;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use crate::net::{Message, message_split};
use futures::stream::{StreamExt};
use tokio::sync::RwLock;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {

    #[clap(long)]
    port: u16,

    #[clap(long, num_args=1..)]
    peers: Vec<u16>,

}


#[derive(Debug, Serialize, Deserialize)]
enum State{
    Primary,
    Follower,
    Candidate,
}

struct Node {
    id: u16,
    state: State,
    peers_ports: Vec<u16>,
}


#[tokio::main]
async fn main() {
    let args: Args = Args::parse();

    if args.port <= 10000 {
        eprintln!("port must be greater than 1000");
        std::process::exit(1);
    }

    if args.peers.len() < 2 {
        eprintln!("at least 2 peers are required");
        std::process::exit(1);
    }

    let node = Arc::new(RwLock::new(
        Node {
            id: args.port,
            state: State::Candidate,
            peers_ports: args.peers,
        }
    ));

    let address = format!("0.0.0.0:{}", args.port);
    println!("Listening on {}", address);
    let listener = TcpListener::bind(address).await.unwrap();
    for peer in node.read().await.peers_ports.iter() {
        let p = peer.clone();
        let n = node.clone();
        tokio::spawn(async move {
            let peer_address = format!("127.0.0.1:{}", p);
            println!("Connecting to {}", peer_address);
            match TcpStream::connect(peer_address.clone()).await {
                Ok(socket) => {
                    let _ = process(socket, n).await;
                }
                Err(err) => {
                    println!("error connecting to server at  --- {peer_address} --- : ERROR({err})");
                }
            };
        });
    }

    loop {
        let (socket, _) = listener.accept().await.unwrap();
        let n = node.clone();
        tokio::spawn(async move {
            let _ = process(socket, n).await;
        });
    }
}

async fn process(sock: TcpStream, node: Arc<RwLock<Node>>) -> Result<(), ErrorKind>{

    println!("New connection from {:?}", &sock.peer_addr().unwrap());
    let (mut rx, mut tx) = message_split(sock);
    tx.send(Message::Handshake {
        id: node.read().await.id,
    }).await.unwrap();

    let result = rx.next().await;
    match result {
        Some(Ok(Message::Handshake { id })) => {
            println!("New Connection established with {}", id);
            Ok(())
        },
        _ => {
            println!("Invalid handshake");
            Err(ErrorKind::InvalidData.into())
        }
    }
}
