use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use futures::SinkExt;
use futures::stream::{FuturesOrdered, StreamExt};
use serde::de::DeserializeOwned;
use tokio::sync::{RwLock};
use crate::communication::net::{message_split};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<M>
    where M: Send + 'static

{
    pub peer_id: u16,
    pub message: MessageBase<M>,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum MessageBase<M>
    where M: Send + 'static

{
    ConnectionFailed { peer: u16 },
    ConnectionBroken { peer: u16 },
    ConnectionEstablished { peer: u16 },
    Custom(M),
}

pub struct Peer<M>
    where M: Send + 'static
{
    pub id: Arc<RwLock<u16>>,
    pub channel: Sender<MessageBase<M>>
}

pub struct NodeCommunication<M>
    where M: Send + 'static

{
    peers_channels: Arc<RwLock<HashMap<u16, Peer<M>>>>,
}

impl<M> NodeCommunication<M>
    where M: Serialize + DeserializeOwned + Send + 'static

{
    pub fn new() -> Self {
        Self {
            peers_channels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start(&mut self, port: u16, peers: Vec<u16>, receiver_channel: Sender<Message<M>>) {
        let address = format!("0.0.0.0:{}", port);
        println!("Listening on {}", address);
        let listener = TcpListener::bind(address).await.unwrap();
        let p_channels = self.peers_channels.clone();
        tokio::spawn(accept_connections(p_channels, listener, receiver_channel.clone()));

        let mut handlers = Vec::new();
        let mut peers_channels = self.peers_channels.write().await;
        for peer in peers.iter() {
            let p = peer.clone();

            let application_channel = receiver_channel.clone();
            let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(100);
            peers_channels.insert(p, Peer {
                id: Arc::new(RwLock::new(p)),
                channel: sender_channel_tx.clone()
            });
            let app_channel = application_channel.clone();

            let peer_address = format!("127.0.0.1:{}", p);
            println!("Connecting to {}", peer_address);
            match TcpStream::connect(peer_address.clone()).await {
                Ok(socket) => {
                    handlers.push(tokio::spawn(start_handler(socket, app_channel, sender_channel_rx, Arc::new(RwLock::new(p)))));
                }
                Err(err) => {
                    println!("error connecting to server at  --- {peer_address} --- : ERROR({err})");
                    let _ = application_channel.send(Message {
                        peer_id: p.clone(),
                        message: MessageBase::ConnectionFailed { peer: p }
                    }).await;
                    peers_channels.remove(&p.clone());
                }
            };
        }
        drop(peers_channels);
    }

    pub async fn send_message(&mut self, peer: u16, message: MessageBase<M>) {
        let read = self.peers_channels.read().await;
        if read.contains_key(&peer) {
            if let Some(peer_lock) = read.get(&peer) {
                let _ = peer_lock.channel.send(message).await;
            } else {
                println!("Peer {} not found!", peer);
            }
        }
        drop(read);
    }

    pub async fn rename_peer(&mut self, old: u16, new: u16) {
        println!("Renaming peer {} to {}", old, new);
        let mut write = self.peers_channels.write().await;
        if let Some(peer) = write.get_mut(&old) {
            {
                let mut id = peer.id.write().await;
                *id = new;
            }

            if let Some(peer_channel) = write.remove(&old) {
                write.insert(new, peer_channel);
                println!("Peer {} renamed to {}", old, new);
            }
        } else {
            println!("Peer {} not found!", old);
        }

    }

    pub async fn get_peers(&self) -> Vec<u16> {
        let read = self.peers_channels.read().await;
        let mut peers: Vec<u16> = read.keys().cloned().collect();
        drop(read);
        peers.sort_by(|a, b| b.cmp(a));
        peers
    }
}

async fn accept_connections<M>(peers_channels: Arc<RwLock<HashMap<u16, Peer<M>>>>, listener: TcpListener, application_channel: Sender<Message<M>>) -> Result<(), ErrorKind>
    where M: Serialize + DeserializeOwned + Send + 'static
{

    let mut handles = FuturesOrdered::new();

    loop {
        tokio::select! {
                Ok((socket, _)) = listener.accept() => {
                    let app_channel = application_channel.clone();
                    let peer_port = socket.peer_addr().unwrap().port();
                    let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(100);
                    let peer_id = Arc::new(RwLock::new(peer_port));

                    {
                        let mut write = peers_channels.write().await;
                        write.insert(peer_port.clone(), Peer {
                            id: peer_id.clone(),
                            channel: sender_channel_tx.clone()

                        });
                        drop(write);
                    }
                    handles.push_back(tokio::spawn(async move { start_handler(socket, app_channel, sender_channel_rx, peer_id.clone()).await }));
                },
                Err(err) = listener.accept() => {
                    eprintln!("Error accepting connection: {:?}", err);
                },
                Some(_) = handles.next() => {}
        }
    }
}

async fn start_handler<M>(socket: TcpStream, channel: Sender<Message<M>>, mut sender_channel_rx: Receiver<MessageBase<M>>, peer: Arc<RwLock<u16>>)
    where M: Serialize + DeserializeOwned + Send + 'static
{
    let (mut rx, mut tx) = message_split(socket);
    {
        let peer_locked = peer.read().await;
        let _ = channel.send(Message {
            peer_id: *peer_locked,
            message: MessageBase::ConnectionEstablished { peer: *peer_locked }
        }).await;
        drop(peer_locked);
    }

    loop {
        tokio::select! {
            Some(message) = rx.next() => {
                match message {
                    Ok(message) => {
                        {
                            let peer_locked = peer.read().await;
                            let  _ = channel.send(Message{
                                peer_id: *peer_locked,
                                message: MessageBase::Custom(message)
                            }).await;
                            drop(peer_locked);
                        }

                    },
                    Err(err) => {
                        {
                            let peer_locked = peer.read().await;
                            eprintln!("Error reading message from socket peer={}: {:?}", *peer_locked, err);
                            let  _ = channel.send(Message{
                                peer_id: *peer_locked,
                                message: MessageBase::ConnectionBroken { peer: *peer_locked }
                            }).await;
                            drop(peer_locked);
                        }
                        break;
                    }
                }
            }
            Some(message) = sender_channel_rx.recv() => {
                match message {
                    MessageBase::Custom(message) => {
                        {
                            let  _ = tx.send(message).await;
                        }
                    },
                    _ => {}
                }
            }
            else => {
                println!("Channels closed!");
                break;
            }
        }
    }
}