use crate::Result;

#[allow(missing_docs)]
/// Multiple threads can access the same KVSEngine allowing parallel execution of the methods below
/// &self allows shared access, meaning multiple threads can call these methods concurrently
/// Clone: Allows creating copies of the engine, which is useful when spawning new threads that each need their own reference
/// Send: Ensures the engine can be safely transferred between threads
/// 'static: Ensures the engine doesn't contain any non-static references, making it safe to use across thread boundaries without lifetime issues
pub trait KvsEngine : Clone + Send + 'static
{
    fn set(&self, key: String, value: String) -> Result<()>;

    fn get(&self, key: String) -> Result<Option<String>>;

    fn remove(&self, key: String) -> Result<()>;
}


mod kv;
mod sled;

pub use self::kv::KvStore;
pub use self::sled::SledKvsEngine;
