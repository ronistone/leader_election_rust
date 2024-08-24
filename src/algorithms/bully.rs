
use serde::{Deserialize, Serialize};

use std::sync::Arc;

use tokio::sync::mpsc::{Receiver};
use tokio::sync::RwLock;
use tokio::time::{Instant, sleep, Duration};
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

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum State {
    Primary,
    Follower,
    Candidate,
}

pub struct Node {
    id: u16,
    state: State,
    leader: Option<u16>,
    node_communication: NodeCommunication<InternalMessage>,
    last_alive: Option<Instant>,
    running_heartbeat: bool,
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
            node_communication: communication,
            last_alive: None,
            running_heartbeat: false,
        }
    ));


    let listener = tokio::spawn(listen_message(node.clone(), channel_rx));

    sleep(Duration::from_millis(2000)).await; // wait for all peers to be connected

    let election = tokio::spawn(start_election(node.clone()));

    let _ = tokio::join!(listener, election);
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
                                let mut write = node.write().await;
                                write.node_communication.rename_peer(message.peer_id, id).await;
                                drop(write);
                            }
                            InternalMessage::Election { id } => {
                                println!("Received Election from peer {}", id);
                                let read = node.read().await;
                                if id < read.id {
                                    drop(read);
                                    tokio::spawn(respond_invalid_election(node.clone(), id));
                                } else {
                                    drop(read);
                                }

                            }
                            InternalMessage::Victory { id } => {
                                println!("Received Victory from peer {}", id);
                                {
                                    let mut write = node.write().await;
                                    let old_leader = write.leader.clone();
                                    write.leader = Some(id);
                                    write.state = State::Follower;
                                    drop(write);

                                    if old_leader.is_none() || old_leader.unwrap() != id {
                                        tokio::spawn(check_leader(node.clone()));
                                    }

                                    println!("The peer {} is the new leader", id);
                                }
                            }
                            InternalMessage::Alive { id } => {
                                {
                                    // println!("Received Alive from peer {}", id); // DEBUG LEADER MESSAGE
                                    let mut lock = node.write().await;
                                    if id > lock.id && lock.state == State::Candidate {
                                        lock.state = State::Follower;
                                    }
                                    if !lock.leader.is_none() && lock.leader.unwrap() == id {
                                        lock.last_alive = Some(Instant::now());
                                    }
                                    drop(lock);
                                }
                            }
                        }
                    }
                    MessageBase::ConnectionEstablished { peer } => {
                        tokio::spawn(handle_handshake(peer, node.clone()));
                    }
                    _ => {
                        eprintln!("Unexpected message: {:?}", message.message)
                    }
                }
            }
            else => {
                println!("Channel closed in listen message!");
            }
        }
    }
}

pub async fn handle_handshake(peer: u16, node: Arc<RwLock<Node>>) {
    let id: u16;
    let leader : Option<u16>;
    println!("Sending Handshake to peer {}", peer);
    {
        let lock = node.read().await;
        id = lock.id;
        leader = lock.leader;
        drop(lock);
    }
    {
        let mut lock = node.write().await;
        let _ = lock.node_communication.send_message(
            peer,
            MessageBase::Custom(InternalMessage::Handshake { id, leader })
        ).await;
        drop(lock);
    }

    sleep(Duration::from_millis(1000)).await;
}

pub async fn respond_invalid_election(node: Arc<RwLock<Node>>, peer_id: u16) {
    {
        println!("Im see invalid election start a new one! peer={}", peer_id);
        let mut lock = node.write().await;
        let node_id = lock.id.clone();
        let _ = lock.node_communication.send_message(
            peer_id,
            MessageBase::Custom(InternalMessage::Alive { id: node_id })
        ).await;
        println!("I Send an alive message to peer {} with my id {}", peer_id, node_id);
        drop(lock);
    }
    let  _ = start_election(node.clone()).await;
}
pub async fn start_election(node: Arc<RwLock<Node>>) {
    let peers: Vec<u16>;
    let node_id: u16;
    let mut lock = node.write().await;
    node_id = lock.id;
    peers = lock.node_communication.get_peers().await;
    lock.state = State::Candidate;
    drop(lock);

    let mut has_send_message = false;
    {
        for peer_id in peers.iter() {
            println!("Sending Election to peer {}", peer_id);
            if *peer_id > node_id {
                let n = node.clone();
                let p = peer_id.clone();
                has_send_message = true;
                tokio::spawn(async move {
                    let mut write = n.write().await;
                    let _ = write.node_communication.send_message(
                        p,
                        MessageBase::Custom(InternalMessage::Election { id: node_id.clone() })
                    ).await;
                    drop(write);
                });
            } else {
                println!("Message not sended! because the peer has a lowest id");
            }
        }
    }
    if has_send_message {
        sleep(Duration::from_millis(3000)).await;
    }
    wait_to_announce_victory(node.clone()).await;
}

pub async fn wait_to_announce_victory(node: Arc<RwLock<Node>>) {

    let read = node.read().await;

    if read.state == State::Candidate {
        drop(read);
        {
            let mut write = node.write().await;
            write.state = State::Primary;
            drop(write);
        }
        println!("Announcing victory");
        let _ = announce_victory(node.clone()).await;
    }

}

async fn announce_victory(node: Arc<RwLock<Node>>) {
    let mut write = node.write().await;
    let peers = write.node_communication.get_peers().await;
    let node_id = write.id.clone();
    for peer_id in peers.iter() {
        println!("Sending Victory to peer {}", peer_id);
        let _ = write.node_communication.send_message(
            *peer_id,
            MessageBase::Custom(InternalMessage::Victory { id: node_id })
        ).await;
    }
    drop(write);
    tokio::spawn(heartbeat(node.clone()));
}

async fn check_leader(node: Arc<RwLock<Node>>) {
    loop {
        let read = node.read().await;
        let state = read.state.clone();
        let last_alive = read.last_alive.clone();
        drop(read);
        if state == State::Primary {
            return;
        }
        if state == State::Follower && !last_alive.is_none() && last_alive.unwrap().elapsed() > Duration::from_secs(1) {
            let _ = start_election(node.clone()).await;
        }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn heartbeat(node: Arc<RwLock<Node>>) {
    let read = node.read().await;
    if read.running_heartbeat {
        drop(read);
        return;
    }
    drop(read);
    loop {
        let read = node.read().await;
        let state = read.state.clone();
        drop(read);
        if state == State::Primary {
            let mut write = node.write().await;
            write.running_heartbeat = true;
            let peers = write.node_communication.get_peers().await;
            let id = write.id.clone();
            for peer_id in peers.iter() {
                if (*peer_id) != id {
                    let _ = write.node_communication.send_message(
                        *peer_id,
                        MessageBase::Custom(InternalMessage::Alive { id })
                    ).await;
                }
            }
            drop(write);
        } else {
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    let mut write = node.write().await;
    write.running_heartbeat = false;
}