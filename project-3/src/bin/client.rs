use std::io::{Read, Write};
use std::mem::size_of;
use std::net::{AddrParseError, SocketAddr, TcpStream};

use clap::{crate_authors, crate_version};
use kvs::MPCommand;
use rmp_serde::Serializer;
use serde::Serialize;
use structopt::StructOpt;

const SIZE_OF_U64: usize = size_of::<u64>() as usize;

#[derive(StructOpt, Debug, Clone)]
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

    let socket: SocketAddr = match opt.clone() {
        Kv::Get { addr, key: _ } => {
            let socket_parse: Result<SocketAddr, AddrParseError> = addr.parse();
            match socket_parse {
                Ok(socket) => socket,
                Err(_err) => {
                    std::process::exit(1);
                }
            }
        }
        Kv::Set {
            addr,
            key: _,
            value: _,
        } => {
            let socket_parse: Result<SocketAddr, AddrParseError> = addr.parse();
            match socket_parse {
                Ok(socket) => socket,
                Err(_err) => {
                    std::process::exit(1);
                }
            }
        }
        Kv::Rm { addr, key: _ } => {
            let socket_parse: Result<SocketAddr, AddrParseError> = addr.parse();
            match socket_parse {
                Ok(socket) => socket,
                Err(_err) => {
                    std::process::exit(1);
                }
            }
        }
    };

    let command: MPCommand = match opt {
        Kv::Get { addr: _, key } => MPCommand::Get { key },
        Kv::Set {
            addr: _,
            key,
            value,
        } => MPCommand::Set { key, value },
        Kv::Rm { addr: _, key } => MPCommand::Rm { key },
    };

    match TcpStream::connect(socket) {
        Ok(mut stream) => {
            stream.write(b"*").unwrap();
            let num_commands = (1 as u64).to_be_bytes();
            stream.write(&num_commands).unwrap();
            let mut serialized_command = Vec::new();
            command
                .serialize(&mut Serializer::new(&mut serialized_command))
                .unwrap();
            let command_length = (serialized_command.len() as u64).to_be_bytes();
            stream.write(&command_length).unwrap();
            stream.write(&serialized_command).unwrap();

            let mut start = [0 as u8; 1];
            stream.read_exact(&mut start).unwrap();
            match start[0] {
                b'*' => {}
                _ => {
                    dbg!("incorrect initial byte");
                }
            }

            let mut num_values = [0 as u8; SIZE_OF_U64];
            stream.read_exact(&mut num_values).unwrap();
            let num_values = u64::from_be_bytes(num_values);

            assert_eq!(num_values, 1);

            let mut error_code = [0 as u8; 1];
            stream.read_exact(&mut error_code).unwrap();

            let mut value_len = [0 as u8; SIZE_OF_U64];

            match error_code[0] {
                b'+' => {
                    stream.read_exact(&mut value_len).unwrap();
                    let value_len = u64::from_be_bytes(value_len);
                    if value_len > 0 {
                        let mut value: Vec<u8> = vec![0; value_len as usize];
                        stream.read_exact(&mut value).unwrap();
                        let value = std::str::from_utf8(&value).unwrap();
                        println!("{}", value);
                    }
                    std::process::exit(0);
                }
                b'-' => {
                    stream.read_exact(&mut value_len).unwrap();
                    let value_len = u64::from_be_bytes(value_len);
                    let mut value: Vec<u8> = vec![0; value_len as usize];
                    stream.read_exact(&mut value).unwrap();
                    let value = std::str::from_utf8(&value).unwrap();
                    eprintln!("{}", value);
                    std::process::exit(1);
                }
                _ => {
                    dbg!("unexpected error code");
                    std::process::exit(1);
                }
            };
        }
        Err(_e) => {
            std::process::exit(1);
        }
    }
}
