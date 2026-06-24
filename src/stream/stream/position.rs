use core::future::Future;
use core::pin::Pin;

use crate::stream::Stream;
use crate::task::{Context, Poll};

#[doc(hidden)]
#[allow(missing_debug_implementations)]
pub struct PositionFuture<'a, S, P> {
    stream: &'a mut S,
    predicate: P,
    index: usize,
}

impl<'a, S, P> Unpin for PositionFuture<'a, S, P> {}

impl<'a, S, P> PositionFuture<'a, S, P> {
    pub(super) fn new(stream: &'a mut S, predicate: P) -> Self {
        Self {
            stream,
            predicate,
            index: 0,
        }
    }
}

impl<'a, S, P> Future for PositionFuture<'a, S, P>
where
    S: Stream + Unpin,
    P: FnMut(S::Item) -> bool,
{
    type Output = Option<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        loop {
            let next = futures_core::ready!(Pin::new(&mut self.stream).poll_next(cx));
            match next {
                Some(v) => {
                    if (&mut self.predicate)(v) {
                        return Poll::Ready(Some(self.index));
                    } else {
                        self.index += 1;
                        continue;
                    }
                }
                None => return Poll::Ready(None),
            }
        }
    }
}
