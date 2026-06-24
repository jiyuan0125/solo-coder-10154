use core::pin::Pin;

use pin_project_lite::pin_project;

use crate::stream::Stream;
use crate::task::{Context, Poll};

pin_project! {
    /// A stream that will repeatedly yield the same list of elements.
    #[derive(Debug)]
    pub struct Cycle<S> {
        orig: S,
        #[pin]
        source: S,
    }
}

impl<S> Cycle<S>
where
    S: Stream + Clone,
{
    pub(crate) fn new(source: S) -> Self {
        Self {
            orig: source.clone(),
            source,
        }
    }
}

impl<S> Stream for Cycle<S>
where
    S: Stream + Clone,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        loop {
            match this.source.as_mut().poll_next(cx) {
                Poll::Ready(None) => {
                    this.source.set(this.orig.clone());
                    continue;
                }
                Poll::Ready(item) => return Poll::Ready(item),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
