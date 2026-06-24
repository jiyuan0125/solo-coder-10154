use std::future::Future;

use crate::io;
use crate::task::{Builder, JoinHandle, Task};

/// Spawns a task.
///
/// This function is similar to [`std::thread::spawn`], except it spawns an asynchronous task.
///
/// [`std::thread`]: https://doc.rust-lang.org/std/thread/fn.spawn.html
///
/// # Examples
///
/// ```
/// # async_std::task::block_on(async {
/// #
/// use async_std::task;
///
/// let handle = task::spawn(async {
///     1 + 2
/// });
///
/// assert_eq!(handle.await.unwrap(), 3);
/// #
/// # })
/// ```
pub fn spawn<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    match Builder::new().spawn(future) {
        Ok(handle) => handle,
        Err(e) => {
            let task = Task::new(None);
            let err = io::Error::new(spawn_error_kind(), e);
            JoinHandle::failed(err, task)
        }
    }
}

pub(crate) fn spawn_error_kind() -> io::ErrorKind {
    io::ErrorKind::Other
}
