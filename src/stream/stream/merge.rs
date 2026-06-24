use core::pin::Pin;
use core::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::stream::stream::StreamExt;
use crate::stream::Fuse;
use crate::stream::Stream;

fn merge_random() -> bool {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u32>> = {
            let mut x = 0i32;
            let r = &mut x;
            let addr = r as *mut i32 as usize;
            Cell::new(Wrapping(addr as u32))
        }
    }

    RNG.with(|rng| {
        let mut x = rng.get();
        x ^= x << 13;
        x ^= x >> 17;
        x ^= x << 5;
        rng.set(x);
        (x.0 & 1) == 0
    })
}

pin_project! {
    /// A stream that merges two other streams into a single stream.
    ///
    /// This `struct` is created by the [`merge`] method on [`Stream`]. See its
    /// documentation for more.
    ///
    /// [`merge`]: trait.Stream.html#method.merge
    /// [`Stream`]: trait.Stream.html
    #[cfg(feature = "unstable")]
    #[cfg_attr(feature = "docs", doc(cfg(unstable)))]
    #[derive(Debug)]
    pub struct Merge<L, R> {
        #[pin]
        left: Fuse<L>,
        #[pin]
        right: Fuse<R>,
        next_left_first: bool,
        left_done: bool,
        right_done: bool,
    }
}

impl<L: Stream, R: Stream> Merge<L, R> {
    pub(crate) fn new(left: L, right: R) -> Self {
        Self {
            left: left.fuse(),
            right: right.fuse(),
            next_left_first: merge_random(),
            left_done: false,
            right_done: false,
        }
    }
}

impl<L, R, T> Stream for Merge<L, R>
where
    L: Stream<Item = T>,
    R: Stream<Item = T>,
{
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        if *this.left_done && *this.right_done {
            return Poll::Ready(None);
        }

        let poll_left_first = *this.next_left_first;
        *this.next_left_first = !*this.next_left_first;

        if poll_left_first {
            if !*this.left_done {
                match this.left.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        *this.left_done = true;
                    }
                    Poll::Pending => {}
                }
            }

            if !*this.right_done {
                match this.right.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        *this.right_done = true;
                    }
                    Poll::Pending => {}
                }
            }
        } else {
            if !*this.right_done {
                match this.right.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        *this.right_done = true;
                    }
                    Poll::Pending => {}
                }
            }

            if !*this.left_done {
                match this.left.as_mut().poll_next(cx) {
                    Poll::Ready(Some(item)) => {
                        return Poll::Ready(Some(item));
                    }
                    Poll::Ready(None) => {
                        *this.left_done = true;
                    }
                    Poll::Pending => {}
                }
            }
        }

        if *this.left_done && *this.right_done {
            Poll::Ready(None)
        } else {
            Poll::Pending
        }
    }
}
