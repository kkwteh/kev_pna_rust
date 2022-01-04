use clap::{crate_authors, crate_version};
use failure;
use kvs::{KvStore, KvsEngine, MPCommand, SledEngine};
use lazy_static::lazy_static;
use rmp_serde;
use slog::{self, error, info, o, Drain, Logger};
use slog_async;
use slog_term;
use std::io::{Read, Write};
use std::mem::size_of;
use std::{
    env::current_dir,
    net::{AddrParseError, SocketAddr, TcpListener, TcpStream},
};
use structopt::StructOpt;

lazy_static! {
    static ref LOGGER: Logger = {
        let decorator = slog_term::TermDecorator::new().stderr().build();
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        slog::Logger::root(drain, o!())
    };
}

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
const SIZE_OF_U64: usize = size_of::<u64>() as usize;
/// Handle tcp connection from client
fn handle_connection(
    mut stream: TcpStream,
    engine: &mut Box<dyn KvsEngine>,
) -> Result<(), failure::Error> {
    // Draw inspiration from Redis protocol
    // let msg = b"*1\r\n$4\r\nPING\r\n";
    // format:
    // - * (indicate start of transmission)
    // - big-endian u64 representing number of commands
    // - + (indicate start of commands)
    // - big-endian u64 representing size in bytes of following command
    // - command
    // - repeat for number of commands
    let mut start = [0 as u8; 1];
    stream.read_exact(&mut start)?;
    match start[0] {
        b'*' => {
            info!(LOGGER, "initiating command sequence");
        }
        _ => {
            info!(LOGGER, "incorrect initial byte");
            return Err(failure::err_msg("incorrect initial byte"));
        }
    }

    let mut num_commands = [0 as u8; SIZE_OF_U64];
    stream.read_exact(&mut num_commands)?;
    let num_commands = u64::from_be_bytes(num_commands);
    info!(
        LOGGER,
        "processing {num_commands} command(s)",
        num_commands = num_commands
    );

    let mut commands: Vec<MPCommand> = vec![];
    for _i in 0..num_commands {
        let mut command_length = [0 as u8; SIZE_OF_U64];
        stream.read_exact(&mut command_length)?;
        let command_length = u64::from_be_bytes(command_length);
        info!(
            LOGGER,
            "processing command with length {command_length}",
            command_length = command_length
        );

        let mut bytes_read: u64 = 0;
        let mut ser_command: Vec<u8> = vec![];
        while bytes_read < command_length {
            let mut data = [0 as u8; 50];
            let size = stream.read(&mut data)?;
            ser_command.extend(&data[0..size].to_vec());
            bytes_read += size as u64;
        }
        let command: MPCommand = rmp_serde::decode::from_read_ref(&ser_command)?;
        info!(LOGGER, "deserialized command");
        dbg!(&command);
        commands.push(command);

        stream.write(b"*")?;
        let num_values = (commands.len() as u64).to_be_bytes();

        stream.write(&num_values)?;
        for command in &commands {
            // TODO: Write value to client
            // format
            // * , num values big_endian u64
            // Ok: + , num_bytes (could be 0), value (string),
            // Err: -, num_bytes, error string (could be binary format as well)
            match command {
                MPCommand::Get { key } => {
                    let value = engine.get(key.clone());
                    match value {
                        Ok(Some(s)) => {
                            // logic
                            stream.write(b"+")?;
                            let string_bytes = s.as_bytes();
                            stream.write(&(string_bytes.len() as u64).to_be_bytes())?;
                            stream.write(string_bytes)?;
                        }
                        Ok(None) => {
                            stream.write(b"+")?;
                            let msg = "Key not found";
                            let msg_bytes = msg.as_bytes();
                            stream.write(&(msg_bytes.len() as u64).to_be_bytes())?;
                            stream.write(msg_bytes)?;
                        }
                        Err(_err) => {
                            stream.write(b"-")?;
                            let msg = "Error getting key";
                            let msg_bytes = msg.as_bytes();
                            stream.write(&(msg_bytes.len() as u64).to_be_bytes())?;
                            stream.write(msg_bytes)?;
                        }
                    }
                }
                MPCommand::Set { key, value } => {
                    let value = engine.set(key.clone(), value.clone());
                    match value {
                        Ok(()) => {
                            stream.write(b"+")?;
                            stream.write(&(0 as u64).to_be_bytes())?;
                        }
                        Err(_err) => {
                            stream.write(b"-")?;
                            let msg = "Error setting key value pair";
                            let msg_bytes = msg.as_bytes();
                            stream.write(&(msg_bytes.len() as u64).to_be_bytes())?;
                            stream.write(msg_bytes)?;
                        }
                    }
                }
                MPCommand::Rm { key } => {
                    let value = engine.remove(key.clone());
                    match value {
                        Ok(()) => {
                            stream.write(b"+")?;
                            stream.write(&(0 as u64).to_be_bytes())?;
                        }
                        Err(err) => match &format!("{}", err)[..] {
                            "Key not found" => {
                                stream.write(b"-")?;
                                let msg = "Key not found";
                                let msg_bytes = msg.as_bytes();
                                stream.write(&(msg_bytes.len() as u64).to_be_bytes())?;
                                stream.write(msg_bytes)?;
                            }
                            err_msg => {
                                error!(LOGGER, "Error {err_msg}", err_msg = err_msg);
                                stream.write(b"-")?;
                                let msg = "Error removing key";
                                let msg_bytes = msg.as_bytes();
                                stream.write(&(msg_bytes.len() as u64).to_be_bytes())?;
                                stream.write(msg_bytes)?;
                            }
                        },
                    }
                }
            }
        }
    }

    Ok(())
}
fn main() {
    let opt = ServerOpt::from_args();

    let socket_parse: Result<SocketAddr, AddrParseError> = opt.addr.parse();
    let socket = match socket_parse {
        Ok(socket) => socket,
        Err(_err) => {
            std::process::exit(1);
        }
    };

    let mut engine: Box<dyn KvsEngine> = match &opt.engine.to_lowercase()[..] {
        "sled" => {
            let cwd = current_dir().unwrap();
            let store = KvStore::open(&cwd.join("my-file")).unwrap();
            Box::new(store)
        }
        "kvs" => {
            let cwd = current_dir().unwrap();
            let sled = SledEngine::open(&cwd).unwrap();
            Box::new(sled)
        }
        _ => {
            std::process::exit(1);
        }
    };

    let version = env!("CARGO_PKG_VERSION");
    info!(LOGGER, "kvs-server version {version}", version = version);
    info!(
        LOGGER,
        "server config: {addr}:{port} {engine_name}",
        addr = socket.ip().to_string(),
        port = socket.port(),
        engine_name = &opt.engine
    );

    let listener = TcpListener::bind(socket).unwrap();

    loop {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    info!(
                        LOGGER,
                        "New connection from: {peer_addr}",
                        peer_addr = stream.peer_addr().unwrap(),
                    );
                    handle_connection(stream, &mut engine).unwrap();
                }
                Err(e) => {
                    info!(LOGGER, "Error: {}", e);
                }
            }
        }
    }
}
