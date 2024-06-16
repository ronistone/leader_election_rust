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

#[tokio::main(flavor="multi_thread", worker_threads=1000)]
async fn main() {
    let args: Args = Args::parse();

    if args.port <= 10000 {
        eprintln!("port must be greater than 10000");
        std::process::exit(1);
    }

    if args.peers.len() < 2 {
        eprintln!("at least 2 peers are required");
        std::process::exit(1);
    }

    algorithms::bully::init_cluster(args.port, args.peers).await;

}