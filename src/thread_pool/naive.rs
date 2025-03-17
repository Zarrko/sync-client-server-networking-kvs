use crate::thread_pool::ThreadPool;
use crate::Result;

/// Naive Threadpool
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        todo!()
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static
    {
        todo!()
    }
}