use core::pin::Pin;
use core::task::{Context, Poll};

use pin_project_lite::pin_project;

use crate::stream::stream::StreamExt;
use crate::stream::Fuse;
use crate::stream::Stream;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn flip(self) -> Side {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }
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
        next_first: Side,
        pending_side: Option<Side>,
        consecutive_same_first: u8,
    }
}

impl<L: Stream, R: Stream> Merge<L, R> {
    pub(crate) fn new(left: L, right: R) -> Self {
        Self {
            left: left.fuse(),
            right: right.fuse(),
            next_first: Side::Left,
            pending_side: None,
            consecutive_same_first: 0,
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
        let this = self.project();

        let first = if let Some(pending) = *this.pending_side {
            pending.flip()
        } else if *this.consecutive_same_first >= 5 {
            this.next_first.flip()
        } else {
            *this.next_first
        };

        if first == *this.next_first {
            *this.consecutive_same_first = this.consecutive_same_first.saturating_add(1);
        } else {
            *this.consecutive_same_first = 1;
        }

        match first {
            Side::Left => {
                poll_in_order(this.left, this.right, cx, this.pending_side, this.next_first, Side::Left, Side::Right)
            }
            Side::Right => {
                poll_in_order(this.right, this.left, cx, this.pending_side, this.next_first, Side::Right, Side::Left)
            }
        }
    }
}

fn poll_in_order<F, S, I>(
    first: Pin<&mut F>,
    second: Pin<&mut S>,
    cx: &mut Context<'_>,
    pending_side: &mut Option<Side>,
    next_first: &mut Side,
    first_side: Side,
    second_side: Side,
) -> Poll<Option<I>>
where
    F: Stream<Item = I>,
    S: Stream<Item = I>,
{
    match first.poll_next(cx) {
        Poll::Ready(Some(item)) => {
            *pending_side = None;
            *next_first = second_side;
            Poll::Ready(Some(item))
        }
        Poll::Ready(None) => second.poll_next(cx),
        Poll::Pending => match second.poll_next(cx) {
            Poll::Ready(Some(item)) => {
                *pending_side = Some(first_side);
                *next_first = second_side;
                Poll::Ready(Some(item))
            }
            Poll::Ready(None) | Poll::Pending => Poll::Pending,
        },
    }
}
