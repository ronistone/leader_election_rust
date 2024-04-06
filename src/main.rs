mod net;
mod internal_communication;

extern crate core;

use std::collections::HashMap;
use std::future::Future;
use std::io::ErrorKind;
use std::sync::{Arc};
use tokio::net::{TcpListener, TcpStream};
use clap::Parser;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use crate::net::{Message, message_split, MessageReader, MessageWriter};
use futures::stream::{FuturesOrdered, StreamExt};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::RwLock;
use crate::internal_communication::InternalMessage;

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
    leader: Option<u16>,
    peers_channels: HashMap<u16, Sender<InternalMessage>>,
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
            leader: None,
            peers_ports: args.peers,
            peers_channels: HashMap::new(),
        }
    ));

    let address = format!("0.0.0.0:{}", args.port);
    println!("Listening on {}", address);
    let listener = TcpListener::bind(address).await.unwrap();

    let (channel_tx, channel_rx) = tokio::sync::mpsc::channel::<InternalMessage>(100);

    let coordinate_spawn = tokio::spawn(coordinate_internal_communication(channel_rx, node.clone()));
    let accept_spawn = tokio::spawn(accept_connections(listener, channel_tx.clone(), node.clone()));
    let peers_connections = tokio::spawn(start_peers_connections(channel_tx, node.clone()));

    tokio::join!(coordinate_spawn, accept_spawn, peers_connections);
}

async fn start_peers_connections(channel_tx: Sender<InternalMessage>, node: Arc<RwLock<Node>>) {
    let peers: Vec<u16>;
    {
        let node_lock = node.read().await;
        peers = node_lock.peers_ports.clone();
    }
    for peer in peers.iter() {
        let p = peer.clone();
        let channel = channel_tx.clone();
        let n = node.clone();
        tokio::spawn(async move {
            let peer_address = format!("127.0.0.1:{}", p);
            println!("Connecting to {}", peer_address);
            match TcpStream::connect(peer_address.clone()).await {
                Ok(socket) => {
                    handle_connection(socket, channel, n).await;
                }
                Err(err) => {
                    println!("error connecting to server at  --- {peer_address} --- : ERROR({err})");
                    channel.send(InternalMessage::ConnectionFailed { peer: p }).await.unwrap();
                }
            };
        });
    }
}

async fn handle_connection(socket: TcpStream, coordinate_channel: Sender<InternalMessage>, node: Arc<RwLock<Node>>) {
    let (mut rx, mut tx) = message_split(socket);
    match handle_handshake(&mut rx, &mut tx, node.clone()).await {
        Ok(channel_rx) => {
            tokio::spawn(process(rx, tx, coordinate_channel.clone(), channel_rx, 0));
        },
        Err(err) => {
            eprintln!("Error handling handshake: {:?}", err);
        }
    }
}

async fn accept_connections(listener: TcpListener, coordinate_channel: Sender<InternalMessage>, node: Arc<RwLock<Node>>) -> Result<(), ErrorKind> {

    let mut handles = FuturesOrdered::new();

    loop {
        tokio::select! {
            Ok((socket, _)) = listener.accept() => {
                handles.push_back(handle_connection(socket, coordinate_channel.clone(), node.clone()));
            },
            Err(err) = listener.accept() => {
                eprintln!("Error accepting connection: {:?}", err);
            },
            Some(_) = handles.next() => {}
        }
    }
}

async fn coordinate_internal_communication(mut channel_rx: Receiver<InternalMessage>, node: Arc<RwLock<Node>>) -> Result<(), ErrorKind>{
    let mut failed = 0;
    loop {
        let message = channel_rx.recv().await.unwrap();
        match message {
            InternalMessage::ConnectionFailed { peer } => {
                failed += 1;
                if failed == node.read().await.peers_ports.len() {
                    println!("All connections failed");
                }
            },
            InternalMessage::ConnectionBroken { peer } => {
                if peer == node.read().await.leader.unwrap() {
                    println!("Connection broken with leader!\nStarting Election!")
                }
            }

            _ => {
                break
            }
        }
    }
    Ok(())
}

async fn handle_handshake(rx: &mut MessageReader<Message, TcpStream>, tx: &mut MessageWriter<Message,TcpStream>, node: Arc<RwLock<Node>>) -> Result<Receiver<InternalMessage>, ErrorKind> {
    let node_id: u16;
    let leader: Option<u16>;
    {
        let node_lock = node.read().await;
        node_id = node_lock.id;
        leader = node_lock.leader
    }

    tx.send(Message::Handshake {
        id: node_id,
        leader,
    }).await.unwrap();

    let result = rx.next().await;
    let channel_rx = match result {
        Some(Ok(Message::Handshake { id, leader: peer_leader })) => {
            println!("New Connection established with {}", id);
            let (channel_tx, channel_rx) = tokio::sync::mpsc::channel::<InternalMessage>(10);
            {
                let mut node_lock = node.write().await;
                node_lock.peers_channels.insert(id, channel_tx);
            }
            if Some(peer_leader) == Some(leader) {
                println!("Leader is the same!");
            } else {
                println!("Leader not the same! Split the brain!?!?!?");
            }
            channel_rx
        },
        _ => {
            println!("Invalid handshake");
            return Err(ErrorKind::InvalidData.into())
        }
    };

    Ok(channel_rx)
}

async fn process(mut rx: MessageReader<Message, TcpStream>, mut tx: MessageWriter<Message,TcpStream>, coordinate_channel: Sender<InternalMessage>, mut receiver_channel: Receiver<InternalMessage>, nodeId: u16) -> Result<(), ErrorKind>{
    // TODO implement bully algorithm
    loop {
        tokio::select! {
            Some(message) = rx.next() => {
                match message {
                    Ok(Message::Propose { id }) => {
                        println!("Received Propose from {}", id);
                        tx.send(Message::Victory { id }).await.unwrap();
                    },
                    Ok(Message::Victory { id }) => {
                        println!("Received Victory from {}", id);
                    },
                    Ok(Message::Refused { highest }) => {
                        println!("Received Refused from {}", highest);
                    },
                    _ => {
                        println!("Invalid message");
                    }
                }
            },
            Some(message) = receiver_channel.recv() => {
                match message {
                    InternalMessage::ConnectionBroken { peer } => {
                        // tx.send(Message::Refused { highest: peer }).await.unwrap();
                        println!("Connection broken with {}", peer);
                    },
                    _ => {
                        println!("Invalid message");
                    }
                }
            }
        }
    }

    // println!("New connection from {:?}", &sock.peer_addr().unwrap());
    // let (mut rx, mut tx) = message_split(sock);
    // tx.send(Message::Handshake {
    //     leader: nodeId,
    // }).await.unwrap();
    //
    // let result = rx.next().await;
    // match result {
    //     Some(Ok(Message::Handshake { id })) => {
    //         println!("New Connection established with {}", id);
    //         Ok(())
    //     },
    //     _ => {
    //         println!("Invalid handshake");
    //         Err(ErrorKind::InvalidData.into())
    //     }
    // }
    Ok(())
}
