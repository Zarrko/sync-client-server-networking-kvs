use clap::{Parser, Subcommand};
use kvs::{KvsClient, Result};
use std::net::SocketAddr;
use std::process::exit;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";

#[derive(Parser, Debug)]
#[clap(name = "kvs-client", disable_help_subcommand = true)]
struct Opt {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    #[clap(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[clap(name = "KEY", help = "A string key")]
        key: String,

        #[clap(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = DEFAULT_LISTENING_ADDRESS,
        )]
        addr: SocketAddr,
    },

    #[clap(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[clap(name = "KEY", help = "A string key")]
        key: String,

        #[clap(name = "VALUE", help = "The string value of the key")]
        value: String,

        #[clap(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = DEFAULT_LISTENING_ADDRESS,
        )]
        addr: SocketAddr,
    },

    #[clap(name = "rm", about = "Remove a given string key")]
    Remove {
        #[clap(name = "KEY", help = "A string key")]
        key: String,

        #[clap(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = DEFAULT_LISTENING_ADDRESS,
        )]
        addr: SocketAddr,
    },
}

fn main() {
    let opt = Opt::parse();
    if let Err(e) = run(opt) {
        eprintln!("{:?}", e);
        exit(1);
    }
}

fn run(opt: Opt) -> Result<()> {
    match opt.command {
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        Command::Remove { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }
    Ok(())
}