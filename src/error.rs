use std::io;
use std::string::FromUtf8Error;

#[derive(Debug)]

/// The KVS Error type
pub enum KvsError {
    /// IO Error
    IoError(io::Error),

    /// Non existent key
    KeyNotFound,

    /// Unexpected Command
    UnexpectedCommandType,

    /// Deserialize error
    Deserialize(prost::DecodeError),

    /// Corrupted data
    CorruptedData,

    /// String error
    StringError(String),

    /// Serialization error
    Serialization(Box<bincode::ErrorKind>),

    /// SledError
    SledError(sled::Error),
}

impl From<io::Error> for KvsError {
    fn from(value: io::Error) -> KvsError {
        KvsError::IoError(value)
    }
}

impl From<prost::DecodeError> for KvsError {
    fn from(value: prost::DecodeError) -> KvsError {
        KvsError::Deserialize(value)
    }
}

impl From<Box<bincode::ErrorKind>> for KvsError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        KvsError::Serialization(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::SledError(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::StringError(err.to_string())
    }
}

/// Result type
pub type Result<T> = std::result::Result<T, KvsError>;