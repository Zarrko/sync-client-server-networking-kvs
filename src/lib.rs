#![deny(missing_docs)]
//! A simple key/value store.

pub use engines::KvStore;
pub use error::{KvsError, Result};

mod engines;
mod error;
mod client;
mod common;
mod server;

#[allow(missing_docs)]
pub mod kvs_command {
    include!(concat!(env!("OUT_DIR"), "/kvs_command.rs"));
}
