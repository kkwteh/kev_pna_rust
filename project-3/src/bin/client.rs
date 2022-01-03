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
    let _opt = Kv::from_args();
}
