use clap::{Arg, Command};
use log::{error, info, LevelFilter};
use std::net::TcpListener;
use std::process;

const DEFAULT_ADDR: &str = "127.0.0.1:4001";
const DEFAULT_ENGINE: &str = "kvs";

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();

    // Parse command-line arguments
    let matches = Command::new("kvs-server")
        .version(env!("CARGO_PKG_VERSION"))
        .arg(
            Arg::new("addr")
                .long("addr")
                .value_name("IP-PORT")
                .help("Sets the listening address"),
        )
        .arg(
            Arg::new("engine")
                .long("engine")
                .value_name("ENGINE-NAME")
                .help("Sets the storage engine"),
        )
        .get_matches();

    // Get address from arguments or use default
    let addr = matches
        .get_one::<String>("addr")
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_ADDR);

    // Get engine from arguments or use default
    let _engine = matches
        .get_one::<String>("engine")
        .map(|s| s.as_str())
        .unwrap_or(DEFAULT_ENGINE);
    // TODO: Validate engine and choose the appropriate implementation

    // Bind to the address
    let listener = match TcpListener::bind(DEFAULT_ADDR) {
        Ok(listener) => listener,
        Err(err) => {
            error!("Failed to bind to {}: {}", addr, err);
            process::exit(1)
        }
    };

    info!("Server listening on {}", addr);

    for stream in listener.incoming() {
        println!("Connection received!");
        match stream {
            Ok(stream) => {
                info!(
                    "Connection established from {}",
                    stream.peer_addr().unwrap()
                );
            }
            Err(e) => {
                error!("Connection Failed: {}", e);
            }
        }
    }
}
