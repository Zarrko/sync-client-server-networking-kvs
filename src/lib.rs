#![deny(missing_docs)]
//! A simple key/value store.

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine, SledKvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;
mod client;
mod common;
mod engines;
mod error;
mod server;

#[allow(missing_docs)]
pub mod thread_pool;

#[allow(missing_docs)]
pub mod kvs_command {
    include!(concat!(env!("OUT_DIR"), "/kvs_command.rs"));
}
