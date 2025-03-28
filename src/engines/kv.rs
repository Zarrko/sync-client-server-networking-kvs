use std::cell::RefCell;
use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::KvsEngine;
use crate::kvs_command::{kvs_command, KvsCommand, KvsRemove, KvsSet};
use crate::{KvsError, Result};
use crc32fast::Hasher;
use prost::Message;
use skiplist::SkipMap;
use std::ffi::OsStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
const CURRENT_SCHEMA_VERSION: u64 = 1;

/// For example, this sequence:
/// store.set("key1", "value1")
/// store.set("key1", "value2")
/// store.remove("key1")
/// Would create log entries like this
// [Length: 4 bytes][KvsCommand: Set with metadata, key1, value1]  // position 0-X
// [Length: 4 bytes][KvsCommand: Set with metadata, key1, value2]  // position X+1-Y
// [Length: 4 bytes][KvsCommand: Remove with metadata, key1]       // position Y+1-Z
/// The in-memory index would:
///
/// First point "key1" to position 0
/// Then update to point to position 41
/// Finally remove the "key1" entry completely
///
/// When the amount of stale data (40 + 41 = 81 bytes in this example) exceeds the COMPACTION_THRESHOLD (1MB), the store performs compaction by:
//
/// Creating a new log file
/// Only copying the latest valid entries
/// Updating the index to point to the new locations
/// Deleting the old log files
///
/// This is why it's called "log-structured" - all operations are simply appended to a log, and compaction handles cleanup of old/stale data.

#[derive(Clone)]
pub struct KvStore {
    // Directory path for the log and other data files
    // Shared between reader and writer components
    path: Arc<PathBuf>,

    // In-memory index mapping keys to their positions in log files
    // Using SkipMap for lock-free concurrent reads
    index: Arc<SkipMap<String, CommandPos>>,
}

/// Manages readonly access to the store.
///
/// Arc (Atomic Reference Counting): Thread-safe shared ownership of a value
/// Allows multiple owners across threads
/// Only deallocates when all references are dropped
///
/// Mutex (Mutual Exclusion) Provides exclusive access to data
/// Only one thread can access the protected data at a time
/// Used to protect KvStoreWriter since writes need exclusive access
/// Blocks other threads until the lock is released
///
/// RefCell Provides interior mutability in a single-threaded context Enforces borrowing rules at runtime (not compile time)
/// Used for readers map to allow mutation through shared references Not thread-safe - only used within a single thread
///
/// SkipMap Lock-free concurrent map implementation Allows multiple readers even during writes
/// Higher performance than a mutex-protected map for read-heavy workloads
/// Used for the key-value index to enable concurrent lookups
///
/// AtomicU64 Thread-safe integer that can be updated atomically Operations don't require locks
/// Used for safe_point to track generation numbers across threads Enables wait-free coordination between readers and writer
struct KvStoreReader {
    // Buffer size for file readers
    reader_buffer_size: usize,

    // Per-thread map of generation numbers to file readers
    // Uses RefCell for interior mutability without thread-safety overhead
    readers: RefCell<HashMap<u64, BufReaderWithPos<File>>>,

    // Atomic generation number indicating the oldest generation that's safe to read
    // Updated during compaction to prevent readers from accessing compacted files
    safe_point: Arc<AtomicU64>,

    // Reader component for handling all read operations
    reader: KvStoreReader,

    // Writer component for handling all write operations
    // Protected by Mutex to ensure exclusive access for writes
    writer: Arc<Mutex<KvStoreWriter>>,
}

/// Manages write operations to the store.
struct KvStoreWriter {
    // Buffer size for file writer
    writer_buffer_size: usize,

    // Current log file write with position tracking
    writer: BufWriterWithPos<File>,

    // current generation for log being written
    current_generation: u64,

    // track bytes of stale commands that can be removed
    uncompacted: u64,

    // Optional sequence number for transactions or entries
    current_sequence: Option<u64>,

    // KvStore Reader
    reader: KvStoreReader,

    // In-memory index mapping keys to their positions in log files
    // Using SkipMap for lock-free concurrent reads
    index: Arc<SkipMap<String, CommandPos>>,

    path: Arc<PathBuf>,
}

impl KvStoreWriter {
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let sequence = self.current_sequence.unwrap_or(0) + 1;
        self.current_sequence = Some(sequence);

        let cmd = KvsCommand::set(key, value, sequence);
        let pos = self.writer.pos;

        let cmd_bytes = cmd.encode_to_vec();

        // Write length prefix (4 bytes, little endian)
        self.writer
            .write_all(&(cmd_bytes.len() as u32).to_le_bytes())?;

        // Write actual message
        self.writer.write_all(&cmd_bytes)?;
        self.writer.flush()?;

        // Update index and track uncompacted bytes
        if let Some(kvs_command::Command::Set(set)) = cmd.command {
            if let Some(old_cmd) = self.index.insert(
                set.key,
                CommandPos {
                    geneeration: self.current_generation,
                    pos,
                    len: self.writer.pos - pos,
                },
            ) {
                self.uncompacted += old_cmd.len;
            }
        }

        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let sequence = self.current_sequence.unwrap_or(0) + 1;
            self.current_sequence = Some(sequence);

            let cmd = KvsCommand::remove(key, sequence);

            let cmd_bytes = cmd.encode_to_vec();

            // Write length prefix (4 bytes, little endian)
            self.writer
                .write_all(&(cmd_bytes.len() as u32).to_le_bytes())?;

            // Write actual message
            self.writer.write_all(&cmd_bytes)?;
            self.writer.flush()?;

            if let Some(command) = cmd.command {
                if let kvs_command::Command::Remove(remove) = command {
                    if let Some(old_cmd) = self.index.remove(&remove.key) {
                        // The remove command itself will be deleted in compaction
                        // once a key is removed, both the original set command and the remove command become "stale"
                        // and can be eliminated during compaction.
                        self.uncompacted += old_cmd.len;
                    }
                }
            }

            if self.uncompacted > COMPACTION_THRESHOLD {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log. And rewrites latest values in a new log file
    pub fn compact(&mut self) -> Result<()> {
        println!(
            "Debug: Starting compaction. Current size: {}",
            self.uncompacted
        );

        // Increase current generation by 2. current_generation + 1 is for the compaction file.
        let compaction_generation = self.current_generation + 1;
        self.current_generation += 2;
        self.writer = self.new_log_file(self.current_generation)?;

        let mut compaction_writer = self.new_log_file(compaction_generation)?;

        let mut new_pos = 0; // Position in the new log file

        // Create a vector to collect keys and positions we need to update
        let mut pos_updates = Vec::new();

        // Iterate through all index entries
        for (key, cmd_pos) in self.index.iter() {
            // Get reader for this generation
            let generation = cmd_pos.geneeration;
            let pos = cmd_pos.pos;
            let len = cmd_pos.len;

            // Access reader through the reader component
            // Note: We need to borrow from RefCell
            let mut readers_borrow = self.reader.readers.borrow_mut();
            let reader = readers_borrow.entry(generation).or_insert_with(|| {
                let path = log_path(&self.path, generation);
                BufReaderWithPos::new(
                    File::open(path).expect("Cannot open log file"),
                    self.reader.reader_buffer_size,
                )
                .expect("Cannot create reader")
            });

            if reader.pos != pos {
                reader.seek(SeekFrom::Start(pos))?;
            }

            // Read length prefix
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let msg_len = u32::from_le_bytes(len_bytes) as usize;

            // Read the message
            let mut msg_bytes = vec![0; msg_len];
            reader.read_exact(&mut msg_bytes)?;

            // Write length prefix to compaction file
            compaction_writer.write_all(&len_bytes)?;

            // Write message bytes to compaction file
            compaction_writer.write_all(&msg_bytes)?;

            // Store the update for this command position
            pos_updates.push((
                key.clone(),
                CommandPos {
                    geneeration: compaction_generation,
                    pos: new_pos,
                    len: 4 + msg_len as u64,
                },
            ));

            new_pos += 4 + msg_len as u64;
        }
        compaction_writer.flush()?;

        // Update the index with the new positions
        for (key, new_cmd_pos) in pos_updates {
            self.index.insert(key, new_cmd_pos);
        }

        // Set the safe point to the compaction generation
        // This is an atomic operation visible to all readers
        let safe_point = Arc::clone(&self.reader.safe_point);
        safe_point.store(compaction_generation, Ordering::SeqCst);

        // Remove stale log files
        let stale_generations: Vec<_> = self
            .reader
            .readers
            .borrow()
            .keys()
            .filter(|&&generation| generation < compaction_generation)
            .cloned()
            .collect();

        for stale_generation in stale_generations {
            // Remove the reader
            self.reader.readers.borrow_mut().remove(&stale_generation);

            // Remove the file
            fs::remove_file(log_path(&self.path, stale_generation))?;
        }

        self.uncompacted = 0;

        Ok(())
    }
}

impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(
        path: impl Into<PathBuf>,
        reader_buffer_size: Option<usize>,
        writer_buffer_size: Option<usize>,
    ) -> Result<KvStore> {
        let reader_buffer_size = reader_buffer_size.unwrap_or(8 * 1024); // 8kb
        let writer_buffer_size = writer_buffer_size.unwrap_or(8 * 1024);
        let path = path.into();
        fs::create_dir_all(&path)?;

        let mut readers = HashMap::new();
        let mut index = BTreeMap::new();

        let mut highest_seq = 0;

        let geneeration_list = sorted_geneeration_list(&path)?;
        let mut uncompacted = 0;

        for &geneeration in &geneeration_list {
            let mut reader = BufReaderWithPos::new(
                File::open(log_path(&path, geneeration))?,
                reader_buffer_size,
            )?;

            let (uncompat, seq) = load_v2(geneeration, &mut reader, &mut index)?;

            uncompacted += uncompat;
            readers.insert(geneeration, reader);
            highest_seq = max(highest_seq, seq);
        }

        let current_geneeration = geneeration_list.last().unwrap_or(&0) + 1;
        let writer = new_log_file(
            &path,
            current_geneeration,
            &mut readers,
            reader_buffer_size,
            writer_buffer_size,
        )?;

        Ok(KvStore {
            path,
            readers,
            writer,
            current_geneeration,
            index,
            uncompacted,
            current_sequence: Some(highest_seq),
            reader_buffer_size,
            writer_buffer_size,
        })
    }

    /// Create a new log file with given geneerationeration number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    fn new_log_file(&mut self, geneeration: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(
            &self.path,
            geneeration,
            &mut self.readers,
            self.writer_buffer_size,
            self.reader_buffer_size,
        )
    }
}

impl KvsEngine for KvStore {
    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(cmd_pos) = self.index.get(&key) {
            let reader = self
                .readers
                .get_mut(&cmd_pos.geneeration)
                .expect("Cannot find log reader");
            reader.seek(SeekFrom::Start(cmd_pos.pos))?;

            // Prefix
            let mut len_bytes = [0u8; 4];
            reader.read_exact(&mut len_bytes)?;
            let msg_len = u32::from_le_bytes(len_bytes) as usize;

            // Read message
            let mut msg_bytes = vec![0; msg_len];
            reader.read_exact(&mut msg_bytes)?;

            let cmd = KvsCommand::decode(&msg_bytes[..])?;
            if !cmd.verify_checksum() {
                return Err(KvsError::CorruptedData);
            }

            if let Some(command) = cmd.command {
                if let kvs_command::Command::Set(set) = command {
                    Ok(Some(set.value))
                } else {
                    Err(KvsError::UnexpectedCommandType)
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}
/// Create a new log file with given geneerationeration number and add the reader to the readers map.
///
/// Returns the writer to the log.
fn new_log_file(
    path: &Path,
    geneeration: u64,
    readers: &mut HashMap<u64, BufReaderWithPos<File>>,
    reader_buffer_size: usize,
    writer_buffer_size: usize,
) -> Result<BufWriterWithPos<File>> {
    let path = log_path(path, geneeration);
    let writer = BufWriterWithPos::new(
        OpenOptions::new().create(true).append(true).open(&path)?,
        writer_buffer_size,
    )?;
    readers.insert(
        geneeration,
        BufReaderWithPos::new(File::open(&path)?, reader_buffer_size)?,
    );
    Ok(writer)
}

/// Returns sorted geneerationeration numbers in the given directory.
fn sorted_geneeration_list(path: &Path) -> Result<Vec<u64>> {
    let mut geneeration_list: Vec<u64> = fs::read_dir(path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    geneeration_list.sort_unstable();
    Ok(geneeration_list)
}

/// Load the whole log file and store value locations in the index map.
///
/// Returns how many bytes can be saved after a compaction.
fn load_v2(
    geneeration: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<(u64, u64)> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut uncompacted = 0;
    let mut highest_sequence = 0;

    loop {
        let start_pos = pos;

        // Read the message length (4 bytes) prefix:
        // 4 bytes (32 bits) allows us to represent message sizes up to ~4GB
        // ToDo: Use variable length encoding like varint
        let mut len_bytes = [0u8; 4];
        match reader.read_exact(&mut len_bytes) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                // reached eof
                break;
            }
            Err(e) => return Err(e.into()),
        }

        let msg_len = u32::from_le_bytes(len_bytes) as usize;
        pos += 4;

        // Read message bytes
        let mut msg_bytes = vec![0u8; msg_len];
        reader.read_exact(&mut msg_bytes)?;
        pos += msg_len as u64;

        // Deserialize the protobuf message
        let cmd = match KvsCommand::decode(&msg_bytes[..]) {
            Ok(cmd) => cmd,
            Err(e) => return Err(KvsError::Deserialize(e)),
        };

        if !cmd.verify_checksum() {
            return Err(KvsError::CorruptedData);
        }

        highest_sequence = max(highest_sequence, cmd.sequence_number);
        match cmd.command {
            Some(kvs_command::Command::Set(set)) => {
                let key = set.key;
                let new_pos = CommandPos {
                    geneeration,
                    pos: start_pos,
                    len: pos - start_pos,
                };

                if let Some(old_cmd) = index.insert(key, new_pos) {
                    uncompacted += old_cmd.len;
                }
            }

            Some(kvs_command::Command::Remove(remove)) => {
                let key = remove.key;
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                // The remove command itself can be deleted in compaction
                uncompacted += pos - start_pos;
            }
            None => {
                return Err(KvsError::UnexpectedCommandType);
            }
        }
    }

    Ok((uncompacted, highest_sequence))
}

fn log_path(dir: &Path, geneeration: u64) -> PathBuf {
    dir.join(format!("{}.log", geneeration))
}

trait Checksumable {
    fn calculate_checksum(&self) -> u32;
    fn get_fields_for_checksum(&self) -> Vec<u8>;
}

impl Checksumable for kvs_command::Command {
    fn calculate_checksum(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.get_fields_for_checksum());
        hasher.finalize()
    }

    fn get_fields_for_checksum(&self) -> Vec<u8> {
        match self {
            _command @ kvs_command::Command::Set(set) => {
                let mut fields = Vec::new();
                fields.extend_from_slice(set.key.as_bytes());
                fields.extend_from_slice(set.value.as_bytes());
                fields
            }

            _command @ kvs_command::Command::Remove(remove) => {
                let mut fields = Vec::new();
                fields.extend_from_slice(remove.key.as_bytes());
                fields
            }
        }
    }
}

impl KvsCommand {
    fn set(key: String, value: String, sequence: u64) -> KvsCommand {
        let command = kvs_command::Command::Set(KvsSet {
            key,
            value,
            key_size: 0,
            value_size: 0,
        });
        let checksum = command.calculate_checksum();
        KvsCommand {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sequence_number: sequence,
            checksum,
            version: CURRENT_SCHEMA_VERSION as u32,
            command: command.into(),
        }
    }

    fn remove(key: String, sequence: u64) -> KvsCommand {
        let command = kvs_command::Command::Remove(KvsRemove { key, key_size: 0 });
        let checksum = command.calculate_checksum();
        KvsCommand {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            sequence_number: sequence,
            checksum,
            version: CURRENT_SCHEMA_VERSION as u32,
            command: command.into(),
        }
    }

    fn verify_checksum(&self) -> bool {
        let stored_checksum = self.checksum;

        let calculated_checksum = match &self.command {
            Some(cmd) => cmd.calculate_checksum(),
            None => return false,
        };

        stored_checksum == calculated_checksum
    }
}

/// Represents the position and length of a json-serialized command in the log.
#[derive(Debug)]
struct CommandPos {
    geneeration: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((geneeration, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            geneeration,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R, buffer_size: usize) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufReaderWithPos {
            reader: BufReader::with_capacity(buffer_size, inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W, buffer_size: usize) -> Result<Self> {
        let pos = inner.stream_position()?;
        Ok(BufWriterWithPos {
            writer: BufWriter::with_capacity(buffer_size, inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
