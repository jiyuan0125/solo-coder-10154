use std::future::Future;
use std::pin::Pin;

use crate::io;
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
    state: Option<HandleState<T>>,
    task: Task,
}

#[cfg(not(target_os = "unknown"))]
type InnerHandle<T> = async_global_executor::Task<T>;
#[cfg(target_arch = "wasm32")]
type InnerHandle<T> = futures_channel::oneshot::Receiver<T>;

#[derive(Debug)]
enum HandleState<T> {
    Running(InnerHandle<T>),
    Failed(io::Error),
    Detached,
}

impl<T> JoinHandle<T> {
    /// Creates a new `JoinHandle`.
    pub(crate) fn new(inner: InnerHandle<T>, task: Task) -> JoinHandle<T> {
        JoinHandle {
            state: Some(HandleState::Running(inner)),
            task,
        }
    }

    /// Creates a new `JoinHandle` representing a failed spawn.
    pub(crate) fn failed(err: io::Error, task: Task) -> JoinHandle<T> {
        JoinHandle {
            state: Some(HandleState::Failed(err)),
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
        match self.state.take() {
            Some(HandleState::Running(handle)) => {
                let result = handle.cancel().await;
                self.state = Some(HandleState::Detached);
                result
            }
            Some(HandleState::Failed(_)) => {
                self.state = Some(HandleState::Detached);
                None
            }
            Some(HandleState::Detached) | None => None,
        }
    }

    /// Cancel this task.
    #[cfg(target_arch = "wasm32")]
    pub async fn cancel(mut self) -> Option<T> {
        match self.state.take() {
            Some(HandleState::Running(mut handle)) => {
                handle.close();
                let result = handle.await.ok();
                self.state = Some(HandleState::Detached);
                result
            }
            Some(HandleState::Failed(_)) => {
                self.state = Some(HandleState::Detached);
                None
            }
            Some(HandleState::Detached) | None => None,
        }
    }
}

#[cfg(not(target_os = "unknown"))]
impl<T> Drop for JoinHandle<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            match state {
                HandleState::Running(handle) => {
                    handle.detach();
                }
                HandleState::Failed(_) | HandleState::Detached => {}
            }
        }
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, io::Error>;

    #[cfg(not(target_os = "unknown"))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        match this.state.take() {
            Some(HandleState::Running(mut handle)) => {
                match Pin::new(&mut handle).poll(cx) {
                    Poll::Ready(result) => {
                        this.state = Some(HandleState::Detached);
                        Poll::Ready(Ok(result))
                    }
                    Poll::Pending => {
                        this.state = Some(HandleState::Running(handle));
                        Poll::Pending
                    }
                }
            }
            Some(HandleState::Failed(err)) => {
                this.state = Some(HandleState::Detached);
                Poll::Ready(Err(err))
            }
            Some(HandleState::Detached) | None => {
                this.state = Some(HandleState::Detached);
                Poll::Pending
            }
        }
    }

    #[cfg(target_arch = "wasm32")]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.as_mut();
        match this.state.take() {
            Some(HandleState::Running(mut handle)) => {
                match Pin::new(&mut handle).poll(cx) {
                    Poll::Ready(Ok(t)) => {
                        this.state = Some(HandleState::Detached);
                        Poll::Ready(Ok(t))
                    }
                    Poll::Ready(Err(_)) => {
                        this.state = Some(HandleState::Detached);
                        Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::Other,
                            "channel was canceled",
                        )))
                    }
                    Poll::Pending => {
                        this.state = Some(HandleState::Running(handle));
                        Poll::Pending
                    }
                }
            }
            Some(HandleState::Failed(err)) => {
                this.state = Some(HandleState::Detached);
                Poll::Ready(Err(err))
            }
            Some(HandleState::Detached) | None => {
                this.state = Some(HandleState::Detached);
                Poll::Pending
            }
        }
    }
}
