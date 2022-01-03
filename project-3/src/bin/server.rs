use clap::{crate_authors, crate_version};
use kvs::{KvStore, KvsEngine, SledEngine};
use std::{
    env::current_dir,
    net::{AddrParseError, SocketAddr},
};
use structopt::StructOpt;

#[derive(StructOpt, Debug, Clone)]
#[structopt(about = "KvServer")]
#[structopt(author = crate_authors!("\n"))]
#[structopt(version = crate_version!())]
struct ServerOpt {
    #[structopt(short, long, default_value = "127.0.0.1:4000")]
    addr: String,

    #[structopt(short, long)]
    engine: String,
}

fn main() {
    let opt = ServerOpt::from_args();
    dbg!(&opt);

    let socket_parse: Result<SocketAddr, AddrParseError> = opt.clone().addr.parse();
    let _socket = match socket_parse {
        Ok(socket) => socket,
        Err(_err) => {
            std::process::exit(1);
        }
    };

    let _engine: Box<dyn KvsEngine> = match &opt.engine.to_lowercase()[..] {
        "sled" => {
            let cwd = current_dir().unwrap();
            let store = KvStore::open(&cwd.join("my-file")).unwrap();
            Box::new(store)
        }
        "kvs" => Box::new(SledEngine {}),
        _ => {
            std::process::exit(1);
        }
    };
}
