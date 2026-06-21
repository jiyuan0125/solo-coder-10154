use std::future::Future;
use std::io;
use std::pin::Pin;

use crate::task::{Context, Poll, Task};

/// A handle that awaits the result of a task.
///
/// Dropping a [`JoinHandle`] will detach the task, meaning that there is no longer
/// a handle to the task and no way to `join` on it.
///
/// Created when a task is [spawned].
///
/// [spawned]: fn.spawn.html
#[derive(Debug)]
pub struct JoinHandle<T> {
    state: JoinState<T>,
    task: Task,
}

#[derive(Debug)]
enum JoinState<T> {
    Running(Option<InnerHandle<T>>),
    Failed(Option<io::Error>),
}

#[cfg(not(target_os = "unknown"))]
type InnerHandle<T> = async_global_executor::Task<T>;
#[cfg(target_arch = "wasm32")]
type InnerHandle<T> = futures_channel::oneshot::Receiver<T>;

impl<T> JoinHandle<T> {
    /// Creates a new `JoinHandle` from a successfully spawned task.
    pub(crate) fn new(inner: InnerHandle<T>, task: Task) -> JoinHandle<T> {
        JoinHandle {
            state: JoinState::Running(Some(inner)),
            task,
        }
    }

    /// Creates a new `JoinHandle` representing a spawn failure.
    pub(crate) fn failed(err: io::Error, task: Task) -> JoinHandle<T> {
        JoinHandle {
            state: JoinState::Failed(Some(err)),
            task,
        }
    }

    /// Returns a handle to the underlying task.
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
    /// println!("id = {}", handle.task().id());
    /// #
    /// # })
    pub fn task(&self) -> &Task {
        &self.task
    }

    /// Cancel this task.
    #[cfg(not(target_os = "unknown"))]
    pub async fn cancel(mut self) -> Option<T> {
        match self.state {
            JoinState::Running(ref mut handle) => {
                let inner = handle.take().unwrap();
                inner.cancel().await
            }
            JoinState::Failed(_) => None,
        }
    }

    /// Cancel this task.
    #[cfg(target_arch = "wasm32")]
    pub async fn cancel(mut self) -> Option<T> {
        match self.state {
            JoinState::Running(ref mut handle) => {
                let mut inner = handle.take().unwrap();
                inner.close();
                inner.await.ok()
            }
            JoinState::Failed(_) => None,
        }
    }
}

#[cfg(not(target_os = "unknown"))]
impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let JoinState::Running(handle) = &mut self.state {
            if let Some(inner) = handle.take() {
                inner.detach();
            }
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, io::Error>;

    #[cfg(not(target_os = "unknown"))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            JoinState::Running(handle) => {
                Pin::new(handle.as_mut().unwrap()).poll(cx).map(Ok)
            }
            JoinState::Failed(err) => {
                Poll::Ready(Err(err.take().unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, "task spawn failed")
                })))
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.state {
            JoinState::Running(handle) => {
                match Pin::new(handle.as_mut().unwrap()).poll(cx) {
                    Poll::Ready(Ok(t)) => Poll::Ready(Ok(t)),
                    Poll::Ready(Err(_)) => Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        "channel was canceled",
                    ))),
                    Poll::Pending => Poll::Pending,
                }
            }
            JoinState::Failed(err) => {
                Poll::Ready(Err(err.take().unwrap_or_else(|| {
                    io::Error::new(io::ErrorKind::Other, "task spawn failed")
                })))
            }
        }
    }
}
