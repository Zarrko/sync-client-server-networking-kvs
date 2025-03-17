use crate::thread_pool::ThreadPool;

/// Shared queue threadpool
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> crate::Result<Self> {
        todo!()
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static
    {
        todo!()
    }
}