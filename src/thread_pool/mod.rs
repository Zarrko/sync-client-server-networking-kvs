mod naive;
mod shared_queue;
mod rayon;

use crate::Result;

pub use self::naive::NaiveThreadPool;
pub use self::shared_queue::SharedQueueThreadPool;
pub use self::rayon::RayonThreadPool;

/// Thread pool
pub trait ThreadPool {
    /// Creates new thread pool with a specific number of threads
    ///
    /// Returns n error if any thread fails to spawn.
    fn new(threads: u32) -> Result<Self> where Self: Sized;

    /// Spawns a function into the threadpool
    ///
    /// Spawning always succeeds, but if the function panics the threadpool continues
    /// to operate with the same number of threads &mdash; the thread count is not
    /// reduced nor is the thread pool destroyed, corrupted or invalidated.
    fn spawn<F>(&self, job: F) where F: FnOnce() + Send + 'static;
}