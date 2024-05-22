
use serde::{Deserialize, Serialize};

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver};
use tokio::sync::RwLock;
use tokio::time::sleep;
use crate::communication::communication::{Message, MessageBase, NodeCommunication};

#[derive(Debug, Serialize, Deserialize)]
pub enum InternalMessage {
    Handshake {
        id: u16,
        leader: Option<u16>,
    },
    Election {
        id: u16,
    },
    Victory {
        id: u16,
    },
    Alive {
        id: u16,
    },
}

// #[derive(Debug, Serialize, Deserialize)]
// pub enum InternalMessage {
//     AllConnectionsFailed {},
//     ConnectionFailed { peer: u16 },
//     ConnectionBroken { peer: u16 },
//     StartElection {},
//     Victory {},
//     PeerElectionTimeout { peer: u16 },
//     LeaderElected { leader: u16 },
//     PeerAlive { peer: u16 },
// }
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum State {
    Primary,
    Follower,
    Candidate,
}

pub struct Node {
    id: u16,
    state: State,
    peers_ports: Vec<u16>,
    leader: Option<u16>,
    node_communication: NodeCommunication<InternalMessage>
}


pub async fn init_cluster(port: u16, peers: Vec<u16>) {
    let (channel_tx, channel_rx) = tokio::sync::mpsc::channel::<Message<InternalMessage>>(100);
    let mut communication = NodeCommunication::new();
    communication.start(port, peers.clone(), channel_tx).await;
    let node = Arc::new(RwLock::new(
        Node {
            id: port,
            state: State::Candidate,
            leader: None,
            peers_ports: peers,
            node_communication: communication,
        }
    ));


    let listener = tokio::spawn(listen_message(node.clone(), channel_rx));

    let _ = tokio::join!(listener);
}

pub async fn listen_message(node: Arc<RwLock<Node>>,
                            mut channel_tx: Receiver<Message<InternalMessage>>) {
    loop {
        tokio::select! {
            Some(message) = channel_tx.recv() => {
                match message.message {
                    MessageBase::Custom(internal_message) => {
                        match internal_message {
                            InternalMessage::Handshake { id, .. } => {
                                println!("Received Handshake from peer {}", id);
                                node.write().await.node_communication.rename_peer(message.peer_id, id).await;
                            }
                            InternalMessage::Election { id } => {
                                println!("Received Election from peer {}", id);
                            }
                            InternalMessage::Victory { id } => {
                                println!("Received Victory from peer {}", id);
                            }
                            InternalMessage::Alive { id } => {
                                println!("Received Alive from peer {} on {}", id, message.peer_id);
                            }
                        }
                    }
                    MessageBase::ConnectionEstablished { peer } => {
                        println!("Connection established with peer {}", peer);
                        handle_handshake(peer, node.clone()).await;
                    }
                    _ => {}
                }
            }
        }
    }
}

pub async fn handle_handshake(peer: u16, node: Arc<RwLock<Node>>) {
    let id: u16;
    let leader : Option<u16>;
    {
        let lock = node.read().await;
        id = lock.id;
        leader = lock.leader;
    }
    let mut lock = node.write().await;
    lock.node_communication.send_message(
        peer,
        MessageBase::Custom(InternalMessage::Handshake { id, leader })
    ).await;

    sleep(Duration::from_millis(500)).await;

    lock.node_communication.send_message(
        peer,
        MessageBase::Custom(InternalMessage::Alive { id })
    ).await;
}