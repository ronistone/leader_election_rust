use std::collections::HashMap;
use std::io::ErrorKind;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::{Receiver, Sender};
use futures::SinkExt;
use futures::stream::{FuturesOrdered, StreamExt};
use serde::de::DeserializeOwned;
use tokio::sync::{Mutex, RwLock};
use crate::communication::net::{message_split, MessageReader, MessageWriter};

#[derive(Debug, Serialize, Deserialize)]
pub struct Message<M>
    where M: Serialize + DeserializeOwned
{
    pub peer_id: u16,
    pub message: MessageBase<M>,
}
#[derive(Debug, Serialize, Deserialize)]
pub enum MessageBase<M>
    where M: Serialize + DeserializeOwned
{
    ConnectionFailed { peer: u16 },
    ConnectionBroken { peer: u16 },
    Custom(M),
}

pub struct NodeCommunication<M>
    where M: Serialize + DeserializeOwned
{
    peers_channels: Arc<RwLock<HashMap<u16, Sender<MessageBase<M>>>>>,
}

impl<M> NodeCommunication<M>
    where M: Serialize + DeserializeOwned
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
        tokio::spawn(self.accept_connections(listener, receiver_channel.clone()));

        let mut handlers = Vec::new();
        let mut peers_channels = HashMap::new();
        for peer in peers.iter() {
            let p = peer.clone();
            let application_channel = receiver_channel.clone();
            let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(100);
            peers_channels.insert(p, sender_channel_tx);
            handlers.push(tokio::spawn(async move {
                let peer_address = format!("127.0.0.1:{}", p);
                println!("Connecting to {}", peer_address);
                match TcpStream::connect(peer_address.clone()).await {
                    Ok(socket) => {
                        let (rx, tx) = message_split(socket);
                        let rx = Arc::new(Mutex::new(rx));
                        let tx = Arc::new(Mutex::new(tx));
                        tokio::spawn(async move { NodeCommunication::start_receiver(rx.clone(), application_channel.clone(), p).await });
                        tokio::spawn(async move { NodeCommunication::start_sender(tx.clone(), sender_channel_rx).await });
                    }
                    Err(err) => {
                        println!("error connecting to server at  --- {peer_address} --- : ERROR({err})");
                        application_channel.send(Message{
                            peer_id: p,
                            message: MessageBase::ConnectionFailed { peer: p }
                        }).await.unwrap();
                    }
                };
            }));
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

    async fn accept_connections(&mut self, listener: TcpListener, application_channel: Sender<Message<M>>) -> Result<(), ErrorKind> {

        let mut handles = FuturesOrdered::new();

        loop {
            tokio::select! {
            Ok((socket, _)) = listener.accept() => {
                handles.push_back(tokio::spawn(async move {
                    let peer_port = socket.peer_addr().unwrap().port();
                    let (rx, tx) = message_split(socket);
                    let rx = Arc::new(Mutex::new(rx));
                    let tx = Arc::new(Mutex::new(tx));

                    tokio::spawn(async move { NodeCommunication::start_receiver(rx.clone(), application_channel.clone(), peer_port).await });
                    tokio::spawn(async move {
                            let (sender_channel_tx, sender_channel_rx) = tokio::sync::mpsc::channel::<MessageBase<M>>(100);
                            NodeCommunication::start_sender(tx.clone(), sender_channel_rx).await;
                            {
                                self.peers_channels.write().await.insert(peer_port, sender_channel_tx.clone());
                            }
                    });
                }));
            },
            Err(err) = listener.accept() => {
                eprintln!("Error accepting connection: {:?}", err);
            },
            Some(_) = handles.next() => {}
        }
        }
    }
    async fn start_receiver(mut rx: Arc<Mutex<MessageReader<M, TcpStream>>>, channel: Sender<Message<M>>, peer: u16) {
        let mut rx_lock = rx.lock().await;
        loop {
            tokio::select! {
                Some(message) = rx_lock.next() => {
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
            }
        }
    }

    async fn start_sender(mut tx: Arc<Mutex<MessageWriter<M, TcpStream>>>, mut sender_channel_rx: Receiver<MessageBase<M>>) {
        loop {
            tokio::select! {
                Some(message) = sender_channel_rx.recv() => {
                    match message {
                        MessageBase::Custom(message) => {
                            {
                                let mut tx_lock = tx.lock().await;
                                tx_lock.send(message).await.unwrap();
                            }
                        },
                        _ => {}
                    }
                }
            }
        }
    }
}