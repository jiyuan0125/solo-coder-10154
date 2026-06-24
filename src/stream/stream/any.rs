use core::marker::PhantomData;
use core::pin::Pin;
use core::future::Future;

use crate::stream::Stream;
use crate::task::{Context, Poll};

#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub struct AnyFuture<'a, S, F, T> {
    pub(crate) stream: &'a mut S,
    pub(crate) f: F,
    pub(crate) _marker: PhantomData<T>,
}

impl<'a, S, F, T> AnyFuture<'a, S, F, T> {
    pub(crate) fn new(stream: &'a mut S, f: F) -> Self {
        Self {
            stream,
            f,
            _marker: PhantomData,
        }
    }
}

impl<S: Unpin, F, T> Unpin for AnyFuture<'_, S, F, T> {}

impl<S, F> Future for AnyFuture<'_, S, F, S::Item>
where
    S: Stream + Unpin + Sized,
    F: FnMut(S::Item) -> bool,
{
    type Output = bool;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let next = futures_core::ready!(Pin::new(&mut *self.stream).poll_next(cx));
            match next {
                Some(v) => {
                    let result = (&mut self.f)(v);
                    if result {
                        return Poll::Ready(true);
                    } else {
                        continue;
                    }
                }
                None => return Poll::Ready(false),
            }
        }
    }
}
