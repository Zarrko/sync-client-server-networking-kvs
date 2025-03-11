use std::io::{BufReader, BufWriter, Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use log::{debug, error, info};
use serde::Serialize;
use crate::common::{GetResponse, RemoveResponse, Request, SetResponse};
use crate::engines::KvsEngine;
use crate::Result;

#[allow(missing_docs)]
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

#[allow(missing_docs)]
impl<E: KvsEngine> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    pub fn run<A: ToSocketAddrs>(mut self, addr: A) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.serve(stream) {
                        error!("Error serving Kvs: {:?}", e);
                    }
                }
                Err(e) => {
                    error!("Error accepting Kvs connection: {:?}", e);
                }
            }
        }

        Ok(())
    }

    fn serve(&mut self, tcp_stream: TcpStream) -> Result<()> {
        let peer_addr = tcp_stream.peer_addr()?;
        let mut reader = BufReader::new(&tcp_stream);
        let mut writer = BufWriter::new(&tcp_stream);

        fn send_response<T: Serialize>(writer: &mut BufWriter<&TcpStream>, resp: T) -> Result<()> {
            let serialized = bincode::serialize(&resp)?;
            let resp_len = serialized.len() as u32;
            writer.write_all(&resp_len.to_be_bytes())?;
            writer.write_all(&serialized)?;
            writer.flush()?;
            Ok(())
        }

        loop {
            // read message length bytes
            let mut len_bytes = [0u8; 4];
            if let Err(e) = reader.read_exact(&mut len_bytes) {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    info!("Client disconnected");
                    break;
                }

                return Err(e.into());
            }

            let len = u32::from_be_bytes(len_bytes) as usize;

            // read serialized request
            let mut buffer = vec![0; len];
            reader.read_exact(&mut buffer)?;

            // Deserialize request
            let request: Request = bincode::deserialize(&buffer)?;

            // Process Request
            match request {
                Request::Get { key } => {
                    let resp = match self.engine.get(key) {
                        Ok(value) => GetResponse::Ok(value),
                        Err(e) => GetResponse::Err(format!("{:?}", e)),
                    };
                    send_response(&mut writer, resp)?;
                },
                Request::Set { key, value} => {
                    let resp = match self.engine.set(key, value) {
                        Ok(_) => SetResponse::Ok(()),
                        Err(e) => SetResponse::Err(format!("{:?}", e))
                    };
                    send_response(&mut writer, resp)?;
                }
                Request::Remove { key } => {
                    let resp = match self.engine.remove(key) {
                        Ok(_) => RemoveResponse::Ok(()),
                        Err(e) => RemoveResponse::Err(format!("{:?}", e))
                    };
                    send_response(&mut writer, resp)?;
                }
            };

            debug!("Response sent to {:?}", peer_addr);
        }

        Ok(())
    }
}