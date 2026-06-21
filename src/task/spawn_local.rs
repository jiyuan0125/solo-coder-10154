use std::future::Future;

use crate::task::{Builder, JoinHandle, Task};

/// Spawns a task onto the thread-local executor.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "unstable")]
/// # async_std::task::block_on(async {
/// #
/// use async_std::task;
///
/// let handle = task::spawn_local(async {
///     1 + 2
/// });
///
/// assert_eq!(handle.await.unwrap(), 3);
/// #
/// # })
/// ```
#[cfg_attr(feature = "docs", doc(cfg(unstable)))]
#[inline]
pub fn spawn_local<F, T>(future: F) -> JoinHandle<T>
where
    F: Future<Output = T> + 'static,
    T: 'static,
{
    match Builder::new().local(future) {
        Ok(handle) => handle,
        Err(err) => JoinHandle::failed(err, Task::new(None)),
    }
}
