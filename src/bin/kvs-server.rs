use clap::{Parser, ValueEnum};
use kvs::*;
use log::LevelFilter;
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::env::current_dir;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::exit;
use std::str::FromStr;

const DEFAULT_LISTENING_ADDRESS: &str = "127.0.0.1:4000";
const DEFAULT_ENGINE: Engine = Engine::kvs;
const CONFIG_FILE_NAME: &str = "kvs_config.toml";

#[derive(Parser, Debug)]
#[clap(name = "kvs-server")]
struct Opt {
    #[clap(
        long,
        help = "Sets the listening address",
        value_name = "IP:PORT",
        default_value = DEFAULT_LISTENING_ADDRESS,
    )]
    addr: SocketAddr,

    #[clap(
        long,
        help = "Sets the storage engine",
        value_name = "ENGINE-NAME",
        value_enum
    )]
    engine: Option<Engine>,
}

// The Engine enum definition
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Engine {
    kvs,
    sled
}

impl std::fmt::Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Engine::kvs => write!(f, "kvs"),
            Engine::sled => write!(f, "sled"),
        }
    }
}

impl FromStr for Engine {
    type Err = KvsError;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "kvs" => Ok(Engine::kvs),
            "sled" => Ok(Engine::sled),
            _ => Err(KvsError::StringError(format!("Unknown engine: {}", s))),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ServerConfig {
    engine: Engine,
    data_dir: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            engine: DEFAULT_ENGINE,
            data_dir: None,
        }
    }
}

fn main() {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    let opt = Opt::parse();

    let res = load_config()
        .and_then(|config| validate_and_run(config, opt));

    if let Err(e) = res {
        KvsError::StringError(format!("{:?}", e));
        exit(1);
    }
}

fn validate_and_run(mut config: ServerConfig, opt: Opt) -> Result<()> {
    // Check if engine is being changed
    if let Some(engine) = opt.engine {
        if config.engine != engine && config.data_dir.is_some() {
            error!("Cannot change engine from {} to {}. Data would be incompatible.",
                   config.engine, engine);
            exit(1);
        }
        config.engine = engine;
    }

    // Set data directory if not already set
    if config.data_dir.is_none() {
        config.data_dir = Some(current_dir()?);
    }

    // Save the updated configuration
    save_config(&config)?;

    run(config, opt.addr)
}

fn run(config: ServerConfig, addr: SocketAddr) -> Result<()> {
    let data_dir = config.data_dir.unwrap();

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", config.engine);
    info!("Listening on {}", addr);

    match config.engine {
        Engine::kvs => run_with_engine(KvStore::open(data_dir, None, None)?, addr),
        Engine::sled => run_with_engine(SledKvsEngine::new(sled::open(data_dir)?), addr),
    }
}

fn run_with_engine<E: KvsEngine>(engine: E, addr: SocketAddr) -> Result<()> {
    let server = KvsServer::new(engine);
    server.run(addr)
}

fn config_path() -> PathBuf {
    current_dir().unwrap_or_default().join(CONFIG_FILE_NAME)
}

fn load_config() -> Result<ServerConfig> {
    let config_path = config_path();

    if !config_path.exists() {
        return Ok(ServerConfig::default());
    }

    let config_content = fs::read_to_string(config_path)?;
    match toml::from_str(&config_content) {
        Ok(config) => Ok(config),
        Err(e) => {
            warn!("Invalid configuration file: {}", e);
            Ok(ServerConfig::default())
        }
    }
}

fn save_config(config: &ServerConfig) -> Result<()> {
    // Explicitly handle the Result to make sure we get the String
    let config_str = match toml::to_string(config) {
        Ok(str) => str,
        Err(e) => return Err(KvsError::StringError(format!("Serialization error: {}", e))),
    };

    // Now config_str is definitely a String
    fs::write(config_path(), config_str)?;
    Ok(())
}