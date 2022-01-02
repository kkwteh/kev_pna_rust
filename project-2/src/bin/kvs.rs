use clap::{crate_authors, crate_version};
use kvs::KvStore;
use std::env::current_dir;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(about = "key value store")]
#[structopt(author = crate_authors!("\n"))]
#[structopt(version = crate_version!())]
enum Kv {
    Get {
        #[structopt(name = "KEY", index = 1)]
        key: String,
    },
    Set {
        #[structopt(name = "KEY", index = 1)]
        key: String,
        #[structopt(name = "VALUE", index = 2)]
        value: String,
    },
    Rm {
        #[structopt(name = "KEY", index = 1)]
        key: String,
    },
}

fn main() {
    let opt = Kv::from_args();

    let cwd = current_dir().unwrap();
    let mut store = KvStore::open(&cwd.join("my-file")).unwrap();
    match opt {
        Kv::Get { key } => match store.get(key) {
            Ok(Some(value)) => {
                println!("{}", value);
                std::process::exit(0);
            }
            Ok(None) => {
                println!("Key not found");
                std::process::exit(0);
            }
            Err(_err) => {
                std::process::exit(1);
            }
        },
        Kv::Set { key, value } => match store.set(key, value) {
            Ok(()) => {
                std::process::exit(0);
            }
            Err(_err) => {
                std::process::exit(1);
            }
        },
        Kv::Rm { key } => match store.remove(key) {
            Ok(()) => {
                std::process::exit(0);
            }
            Err(_err) => {
                println!("Key not found");
                std::process::exit(1);
            }
        },
    }
}
