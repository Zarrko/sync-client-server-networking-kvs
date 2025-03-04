use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::{KvsError, Result};
use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use serde::{Deserialize, Serialize};

pub struct KvsClient {
    reader: BufReader<TcpStream>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    pub fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self> {
        let tcp_reader = TcpStream::connect(addr)?;
        let tcp_writer = tcp_reader.try_clone()?;
        Ok(KvsClient {
            reader: BufReader::new(tcp_reader),
            writer: BufWriter::new(tcp_writer),
        })
    }

    fn send_request<T: Serialize>(&mut self, request: T) -> Result<()>{
        let serialized = bincode::serialize(&request)?;

        // Send length prefix followed by data
        let len = serialized.len() as u32;
        self.writer.write_all(&len.to_be_bytes())?;
        self.writer.write_all(&serialized)?;
        self.writer.flush()?;

        Ok(())
    }

    fn receive_request<T: for<'de> Deserialize<'de>>(&mut self) -> Result<T> {
        // Read response
        let mut len_bytes = [0u8; 4]; // 4 bytes == largest possible integer
        self.reader.read_exact(&mut len_bytes)?;
        let len = u32::from_be_bytes(len_bytes) as usize;

        // Read and deserialize the response
        let mut buf = vec![0; len];
        self.reader.read_exact(&mut buf)?;
        let result= bincode::deserialize(&buf)?;

        Ok(result)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        self.send_request(Request::Get { key })?;

        let result: GetResponse = self.receive_request()?;
        match result {
            GetResponse::Ok(resp) => Ok(resp),
            GetResponse::Err(e) => Err(KvsError::StringError(e)),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
       self.send_request(Request::Set {key, value})?;

        let result: SetResponse = self.receive_request()?;
        match result {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        self.send_request(Request::Remove { key })?;

        let result: RemoveResponse = self.receive_request()?;
        match result {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(msg) => Err(KvsError::StringError(msg)),
        }
    }
}