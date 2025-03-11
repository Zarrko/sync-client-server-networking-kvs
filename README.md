# Rust Key-Value Store with Custom Network Protocol
This project implements a networked key-value store in Rust with a focus on building a solid foundation for database concepts. It includes a custom binary protocol, multiple storage engine options, and a client-server architecture.

## Features

- Client-server architecture using Rust's TCP networking capabilities
- Custom binary protocol with length-prefixed messages for efficient communication
- Support for multiple storage engines:
  -   Custom KvStore log-structured storage engine
  -   Sled embedded database integration (a log-structured merge tree implementation)
- Persistent configuration management
- Command-line interfaces for both client and server components

## Running the Server
Run with default settings (127.0.0.1:4000, kvs engine)
`cargo run --bin kvs-server`

Run with custom settings
`cargo run --bin kvs-server -- --addr 127.0.0.1:5000 --engine sled`

## Running the Client 
Set a key-value pair
`cargo run --bin kvs-client -- set mykey myvalue`

Get a value
`cargo run --bin kvs-client -- get mykey`

Remove a key
`cargo run --bin kvs-client -- rm mykey`

Connect to a custom server address
`cargo run --bin kvs-client -- get mykey --addr 127.0.0.1:5000`

## Binary Protocol Design
The project implements a custom binary protocol using:

- Efficient binary serialization with bincode
- Length-prefixed messages (4-byte headers) for proper framing
- Type-safe request and response types

## Storage Engines
### Custom KvStore
A simple log-structured key-value store that writes operations sequentially and periodically compacts the log to reclaim space.


### Sled Integration
An embedded database using Log-Structured Merge Trees (LSM trees):

- In-memory memtables for efficient writes
- Persistent SSTables (Sorted String Tables) for storage
- Automatic compaction for space reclamation
