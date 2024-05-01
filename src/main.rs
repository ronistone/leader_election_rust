extern crate core;

use clap::Parser;

mod algorithms;
mod communication;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {

    #[clap(long)]
    port: u16,

    #[clap(long, num_args=1..)]
    peers: Vec<u16>,

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

    algorithms::bully::init_cluster(args.port, args.peers).await;
}

// async fn start_peers_connections(channel_tx: Sender<InternalMessage>, node: Arc<RwLock<Node>>) {
//
//     let _ = channel_tx.send(InternalMessage::StartElection{}).await;
// }

// async fn coordinate_internal_communication(mut channel_rx: Receiver<InternalMessage>, node: Arc<RwLock<Node>>) -> Result<(), ErrorKind>{
//     let mut failed = 0;
//     loop {
//         let message = channel_rx.recv().await.unwrap();
//         match message {
//             InternalMessage::StartElection {} =>  {
//                 println!("Starting Election!");
//                 {
//                     let mut node_lock = node.write().await;
//                     node_lock.state = State::Candidate;
//                     node_lock.leader = Some(node_lock.id);
//                     let mut is_high_peer = true;
//                     for (peer, channel) in node_lock.peers_channels.iter() {
//                         if *peer > node_lock.id {
//                             channel.send(InternalMessage::StartElection {}).await.unwrap();
//                             is_high_peer = false;
//                         }
//                     }
//                     if is_high_peer {
//                         println!("I am the leader!");
//                         node_lock.state = State::Primary;
//                         for (_, channel) in node_lock.peers_channels.iter() {
//                             channel.send(InternalMessage::Victory {}).await.unwrap();
//                         }
//                     }
//                 }
//             },
//             InternalMessage::PeerAlive { peer } => {
//                 {
//                     let mut node_lock = node.write().await;
//                     if node_lock.state == State::Candidate {
//                         node_lock.state = State::Follower;
//                         node_lock.leader = Some(peer);
//                     }
//                 }
//             },
//             InternalMessage::LeaderElected { leader } => {
//                 {
//                     let mut node_lock = node.write().await;
//                     node_lock.state = State::Follower;
//                     node_lock.leader = Some(leader);
//                 }
//                 println!("New Leader elected: {}", leader);
//             },
//             InternalMessage::ConnectionFailed { peer: _peer } => {
//                 failed += 1;
//                 if failed == node.read().await.peers_ports.len() {
//                     println!("All connections failed");
//                 }
//             },
//             InternalMessage::ConnectionBroken { peer } => {
//                 if peer == node.read().await.leader.unwrap() {
//                     println!("Connection broken with leader!\nStarting Election!")
//                 }
//             }
//
//             _ => {
//                 break
//             }
//         }
//     }
//     Ok(())
// }
//

//
// async fn handle_handshake(rx: &mut MessageReader<Message, TcpStream>, tx: &mut MessageWriter<Message,TcpStream>, node: Arc<RwLock<Node>>) -> Result<(Receiver<InternalMessage>,u16,u16), ErrorKind> {
//     let node_id: u16;
//     let peer_id: u16;
//     let leader: Option<u16>;
//     {
//         let node_lock = node.read().await;
//         node_id = node_lock.id;
//         leader = node_lock.leader
//     }
//
//     println!("SENDING HANDSHAKE!");
//     let _ = tx.send(Message::Handshake {
//         id: node_id,
//         leader,
//     }).await;
//
//     let result = rx.next().await;
//     let channel_rx = match result {
//         Some(Ok(Message::Handshake { id, leader: peer_leader })) => {
//             println!("New Connection established with {}", id);
//             let (channel_tx, channel_rx) = tokio::sync::mpsc::channel::<InternalMessage>(10);
//             {
//                 let mut node_lock = node.write().await;
//                 node_lock.peers_channels.insert(id, channel_tx);
//             }
//             peer_id = id;
//             if Some(peer_leader) == Some(leader) {
//                 println!("Leader is the same!");
//             } else {
//                 println!("Leader not the same! Split the brain!?!?!? {:?} != {:?}", peer_leader, leader);
//             }
//             channel_rx
//         },
//         _ => {
//             println!("Invalid handshake");
//             return Err(ErrorKind::InvalidData.into())
//         }
//     };
//     println!("Handshake completed!");
//     Ok((channel_rx, peer_id, node_id))
// }
//
// async fn process(mut rx: MessageReader<Message, TcpStream>, mut tx: MessageWriter<Message,TcpStream>, coordinate_channel: Sender<InternalMessage>, mut receiver_channel: Receiver<InternalMessage>, node_id: u16, peer_id: u16) -> Result<(), ErrorKind>{
//     let coordinate_channel_socket = coordinate_channel.clone();
//     let socket_rx = rx.clone();
//     let socket_tx = tx.clone();
//     let mut is_await_election = Arc::new(RwLock::new(false));
//     let mut socket_is_await_election = is_await_election.clone();
//     let socket_spawn = tokio::spawn(async move {
//         loop {
//             tokio::select! {
//                 Some(message) = socket_rx.next() => {
//                     match message {
//                         Ok(Message::Election { id }) => {
//                             println!("Received Propose from {}", id);
//                             if node_id > id {
//                                 println!("SENDING ALIVE!");
//                                 if let Err(_) = socket_tx.send(Message::Alive { id: 0 }).await {
//                                     return broken_connection(peer_id, coordinate_channel_socket.clone()).await;
//                                 }
//                             }
//                         },
//                         Ok(Message::Victory { id }) => {
//                             println!("Received Victory from {}", id);
//                             {
//                                 let lock = socket_is_await_election.write().await;
//                                 *lock = false;
//                             }
//                             if node_id < id {
//                                 coordinate_channel_socket.send(InternalMessage::LeaderElected { leader: id }).await.unwrap();
//                             } else {
//                                 coordinate_channel_socket.send(InternalMessage::StartElection {}).await.unwrap();
//                             }
//                         },
//                         Ok(Message::Alive { id: 0 }) => {
//                             println!("Received Alive from {}", peer_id);
//                             coordinate_channel_socket.send(InternalMessage::PeerAlive { peer: peer_id }).await.unwrap()
//                         },
//                         Err(e) => {
//                             println!("Disconnecting {} with err {}", peer_id, e);
//                             coordinate_channel_socket.send(InternalMessage::ConnectionBroken { peer: peer_id }).await.unwrap();
//                             return Ok(())
//                         },
//                         _ => {
//                             println!("Invalid message");
//                         }
//                     }
//                 },
//                 Some(message) = tokio::time::timeout(time::Duration::from_secs(1), socket_rx.next()).await, if socket_is_await_election.read().await == true => {
//                     return match message {
//                         Err(_) => {
//                             coordinate_channel_socket.send(InternalMessage::PeerElectionTimeout { peer: peer_id }).await.unwrap();
//                             Err(ErrorKind::ConnectionReset)
//                         },
//                     }
//                 },
//             }
//
//         }
//     });
//     let receiver_spawn = tokio::spawn(async move {
//         loop {
//             let Some(message) = receiver_channel.recv().await;
//             match message {
//                 InternalMessage::StartElection {} => {
//                     println!("SENDING ELECTION!");
//                     if let Err(_) = tx.send(Message::Election { id: node_id }).await {
//                         return broken_connection(peer_id, coordinate_channel.clone()).await;
//                     }
//                     {
//                         let lock = is_await_election.write().await;
//                         *lock = true;
//                     }
//                 }
//                 InternalMessage::Victory {} => {
//                     println!("SENDING VICTORY!");
//                     if let Err(_) = tx.send(Message::Victory { id: node_id }).await {
//                         return broken_connection(peer_id, coordinate_channel.clone()).await;
//                     }
//                 },
//                 _ => {
//                     println!("Invalid message");
//                 }
//             };
//         }
//     });
//     tokio::join!(socket_spawn, receiver_spawn);
//     return Ok(())
// }
//
// async fn broken_connection(peer_id: u16, coordinate_channel: Sender<InternalMessage>) -> Result<(), ErrorKind> {
//     coordinate_channel.send(InternalMessage::ConnectionBroken { peer: peer_id }).await.unwrap();
//     Err(ErrorKind::ConnectionReset)
// }
