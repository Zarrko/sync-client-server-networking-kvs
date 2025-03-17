use crate::thread_pool::ThreadPool;

/// Rayon threadpool
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
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