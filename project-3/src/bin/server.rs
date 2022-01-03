use clap::{crate_authors, crate_version};
use kvs::{KvStore, KvsEngine, SledEngine};
use slog::{self, info, o, Drain};
use slog_async;
use slog_term;
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
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();

    let logger = slog::Logger::root(drain, o!());

    let opt = ServerOpt::from_args();
    dbg!(&opt);

    let version = env!("CARGO_PKG_VERSION");
    info!(logger, "kvs-server version {version}", version = version);

    let socket_parse: Result<SocketAddr, AddrParseError> = opt.addr.parse();
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
