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
    Custom(M),
}

pub struct NodeCommunication<M>
    where M: Send + 'static

{
    peers_channels: Arc<RwLock<HashMap<u16, Sender<MessageBase<M>>>>>,
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
        let mut peers_channels = HashMap::new();
        for peer in peers.iter() {
            let p = peer.clone();
            let application_channel = receiver_channel.clone();
            let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(100);
            peers_channels.insert(p, sender_channel_tx);
            let app_channel = application_channel.clone();

            let peer_address = format!("127.0.0.1:{}", p);
            println!("Connecting to {}", peer_address);
            match TcpStream::connect(peer_address.clone()).await {
                Ok(socket) => {
                    handlers.push(tokio::spawn(async move { start_handler(socket, app_channel, sender_channel_rx, p).await }));
                }
                Err(err) => {
                    println!("error connecting to server at  --- {peer_address} --- : ERROR({err})");
                    application_channel.send(Message{
                        peer_id: p,
                        message: MessageBase::ConnectionFailed { peer: p }
                    }).await.unwrap();
                }
            };
        }

        for handler in handlers {
            handler.await.unwrap();
        }
    }

    pub async fn send_message(&mut self, peer: u16, message: MessageBase<M>) {
        let read = self.peers_channels.read().await;
        if read.contains_key(&peer) {
            let peer_channel = read.get(&peer).unwrap();
            peer_channel.send(message).await.unwrap();
        }
    }

    pub async fn rename_peer(&mut self, old: u16, new: u16) {
        let mut write = self.peers_channels.write().await;
        if write.contains_key(&old) {
            let peer_channel = write.remove(&old).unwrap();
            write.insert(new, peer_channel);
        }
    }
}

async fn accept_connections<M>(peers_channels: Arc<RwLock<HashMap<u16, Sender<MessageBase<M>>>>>, listener: TcpListener, application_channel: Sender<Message<M>>) -> Result<(), ErrorKind>
    where M: Serialize + DeserializeOwned + Send + 'static
{

    let mut handles = FuturesOrdered::new();

    loop {
        tokio::select! {
                Ok((socket, _)) = listener.accept() => {
                    let app_channel = application_channel.clone();
                    let peer_port = socket.peer_addr().unwrap().port();
                    let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(100);
                    {
                        peers_channels.write().await.insert(peer_port.clone(), sender_channel_tx.clone());
                    }
                    let p = peer_port.clone();
                    handles.push_back(tokio::spawn(async move { start_handler(socket, app_channel, sender_channel_rx, p).await }));
                },
                Err(err) = listener.accept() => {
                    eprintln!("Error accepting connection: {:?}", err);
                },
                Some(_) = handles.next() => {}
            }
    }
}

async fn start_handler<M>(socket: TcpStream, channel: Sender<Message<M>>, mut sender_channel_rx: Receiver<MessageBase<M>>, peer: u16)
    where M: Serialize + DeserializeOwned + Send + 'static
{
    let (mut rx, mut tx) = message_split(socket);
    loop {
        tokio::select! {
            Some(message) = rx.next() => {
                match message {
                    Ok(message) => {
                        channel.send(Message{
                            peer_id: peer,
                            message: MessageBase::Custom(message)
                        }).await.unwrap();
                    },
                    Err(err) => {
                        eprintln!("Error reading message from socket: {:?}", err);
                        // TODO peer can be renamed when connection is accepted and we dont know the peer id yet, need alter to handle the peer rename
                        // maybe use an map with peer id to peer port and if not found in this map, the handshake was not done yet
                        channel.send(Message {
                            peer_id: peer,
                            message: MessageBase::ConnectionBroken { peer }
                        }).await.unwrap();
                        break;
                    }
                }
            }
            Some(message) = sender_channel_rx.recv() => {
                match message {
                    MessageBase::Custom(message) => {
                        {
                            tx.send(message).await.unwrap();
                        }
                    },
                    _ => {}
                }
            }
        }
    }
}