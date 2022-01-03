use std::net::{AddrParseError, SocketAddr};

use clap::{crate_authors, crate_version};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(about = "key value store")]
#[structopt(author = crate_authors!("\n"))]
#[structopt(version = crate_version!())]
enum Kv {
    Get {
        #[structopt(name = "KEY", index = 1)]
        key: String,
        #[structopt(short, long, default_value = "127.0.0.1:4000")]
        addr: String,
    },
    Set {
        #[structopt(name = "KEY", index = 1)]
        key: String,
        #[structopt(name = "VALUE", index = 2)]
        value: String,
        #[structopt(short, long, default_value = "127.0.0.1:4000")]
        addr: String,
    },
    Rm {
        #[structopt(name = "KEY", index = 1)]
        key: String,
        #[structopt(short, long, default_value = "127.0.0.1:4000")]
        addr: String,
    },
}

fn main() {
    let opt = Kv::from_args();
    dbg!(&opt);

    match opt {
        Kv::Get { addr, key } => {
            let socket_parse: Result<SocketAddr, AddrParseError> = addr.parse();
            let _socket = match socket_parse {
                Ok(socket) => socket,
                Err(_err) => {
                    std::process::exit(1);
                }
            };
        }
        Kv::Set { addr, key, value } => {
            let socket_parse: Result<SocketAddr, AddrParseError> = addr.parse();
            let _socket = match socket_parse {
                Ok(socket) => socket,
                Err(_err) => {
                    std::process::exit(1);
                }
            };
        }
        Kv::Rm { addr, key } => {
            let socket_parse: Result<SocketAddr, AddrParseError> = addr.parse();
            let _socket = match socket_parse {
                Ok(socket) => socket,
                Err(_err) => {
                    std::process::exit(1);
                }
            };
        }
    }
}
