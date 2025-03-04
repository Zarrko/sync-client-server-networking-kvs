use clap::{Arg, ArgAction, Command};
use log::info;
use log::LevelFilter;
use std::net::TcpStream;
use std::process;

const DEFAULT_ADDR: &str = "127.0.0.1:4000";

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    // Parse command-line arguments
    let matches = Command::new("kvs-client")
        .subcommand(
            Command::new("set")
                .about("Set the value of a key")
                .disable_version_flag(true)
                .arg(Arg::new("KEY").required(true))
                .arg(Arg::new("VALUE").required(true))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .help("Server address"),
                ),
        )
        .subcommand(
            Command::new("get")
                .about("Get the value of a key")
                .disable_version_flag(true)
                .arg(Arg::new("KEY").required(true))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .help("Server address"),
                ),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove a key")
                .disable_version_flag(true)
                .arg(Arg::new("KEY").required(true))
                .arg(
                    Arg::new("addr")
                        .long("addr")
                        .value_name("IP-PORT")
                        .help("Server address"),
                ),
        )
        .arg(
            Arg::new("version")
                .short('V')
                .long("version")
                .action(ArgAction::SetFalse)
                .help("Print version info and exit"),
        )
        .get_matches();

    // Handle version flag
    if matches.get_flag("version") {
        println!("kvs-client {}", env!("CARGO_PKG_VERSION"));
        return;
    }

    // Extract the subcommand and handle it
    match matches.subcommand() {
        Some(("set", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("KEY")
                .expect("KEY is required");
            let value = sub_matches
                .get_one::<String>("VALUE")
                .expect("VALUE is required");
            let addr = sub_matches
                .get_one::<String>("addr")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_ADDR);

            println!("Client connecting...");
            execute_set(addr, key, value);
        }
        Some(("get", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("KEY")
                .expect("KEY is required");
            let addr = sub_matches
                .get_one::<String>("addr")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_ADDR);

            execute_get(addr, key);
        }
        Some(("rm", sub_matches)) => {
            let key = sub_matches
                .get_one::<String>("KEY")
                .expect("KEY is required");
            let addr = sub_matches
                .get_one::<String>("addr")
                .map(|s| s.as_str())
                .unwrap_or(DEFAULT_ADDR);

            execute_remove(addr, key);
        }
        _ => {
            eprintln!("Error: missing subcommand");
            process::exit(1);
        }
    }
}

fn execute_set(addr: &str, _key: &str, _value: &str) {
    match TcpStream::connect(addr) {
        Ok(mut _stream) => {
            // TODO: Implement your protocol to send a SET command
            // For example:
            // write!(stream, "SET {} {}\n", key, value).unwrap();

            // TODO: Read and process the response
            // let mut response = String::new();
            // stream.read_to_string(&mut response).unwrap();
            // if response.trim() == "OK" { ... } else { ... }

            info!("Connected to {}", addr);
        }
        Err(e) => {
            eprintln!("Error: failed to connect to {}: {}", addr, e);
            process::exit(1);
        }
    }
}

fn execute_get(addr: &str, _key: &str) {
    match TcpStream::connect(addr) {
        Ok(mut _stream) => {
            // TODO: Implement your protocol to send a GET command
            // TODO: Read and process the response
            info!("Connected to {}", addr);
        }
        Err(e) => {
            eprintln!("Error: failed to connect to {}: {}", addr, e);
            process::exit(1);
        }
    }
}

fn execute_remove(addr: &str, _key: &str) {
    match TcpStream::connect(addr) {
        Ok(mut _stream) => {
            // TODO: Implement your protocol to send a REMOVE command
            // TODO: Read and process the response
            info!("Connected to {}", addr);
        }
        Err(e) => {
            eprintln!("Error: failed to connect to {}: {}", addr, e);
            process::exit(1);
        }
    }
}
