use std::pin::Pin;
use std::task::{Context, Poll};

use async_std::stream::{self, Stream, StreamExt};
use async_std::task;

struct AltReady {
    side: u8,
    count: u32,
    max: u32,
}

impl Stream for AltReady {
    type Item = u8;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.count += 1;
        if self.count > self.max {
            return Poll::Ready(None);
        }
        // alternate: odd polls = Ready, even = Pending (for left)
        if self.count % 2 == 1 {
            Poll::Ready(Some(self.side))
        } else {
            Poll::Pending
        }
    }
}

struct AltReadyOffset {
    side: u8,
    count: u32,
    max: u32,
}

impl Stream for AltReadyOffset {
    type Item = u8;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.count += 1;
        if self.count > self.max {
            return Poll::Ready(None);
        }
        // opposite phase: odd = Pending, even = Ready
        if self.count % 2 == 0 {
            Poll::Ready(Some(self.side))
        } else {
            Poll::Pending
        }
    }
}

fn main() {
    eprintln!("=== CP-12 test: alternating merge fairness ===");
    // The P01 says: left=Ready-then-Pending, right=Pending-then-Ready
    // Strict alternation next_left_first guarantees equal distribution
    // This test must avoid deadlock by ensuring each merge poll returns Some or wakes
    // Use direct poll instead of .await
    use std::future::Future;
    use std::task::{RawWaker, RawWakerVTable, Waker};
    fn dummy_waker() -> Waker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, no_op, no_op, no_op);
        unsafe { Waker::from_raw(clone(std::ptr::null())) }
    }
    let waker = dummy_waker();
    let mut ctx = Context::from_waker(&waker);

    // Track which side was polled first on each merge.poll_next
    let left = AltReady { side: 0, count: 0, max: 1000 };
    let right = AltReadyOffset { side: 1, count: 0, max: 1000 };
    let mut merged = left.merge(right);
    let mut side_first: Vec<u8> = vec![];
    let mut total_left_first = 0;
    let mut total_right_first = 0;

    for _ in 0..200 {
        match Pin::new(&mut merged).poll_next(&mut ctx) {
            Poll::Ready(Some(_)) => {
                // skip - we want to track pending/empty transitions
            }
            Poll::Ready(None) => break,
            Poll::Pending => {
                // Couldn't poll both streams without pending due to AlternatingPhase - but
                // since left side returns Ready every other poll and right side every other (opposite),
                // on any given merge.poll_next, after polling first side (say left=Pending), poll second (right=Ready), return Ready.
                // So we never hit Pending in this setup. Let's verify by tracking first side.
            }
        }
    }

    // Better: just check that merge gives interleaved output, using a different stream design
    // Stream that always returns Ready(Some(side))
    struct AlwaysReady { side: u8, count: u32, max: u32 }
    impl Stream for AlwaysReady {
        type Item = u8;
        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.count += 1;
            if self.count > self.max { return Poll::Ready(None); }
            Poll::Ready(Some(self.side))
        }
    }

    let left = AlwaysReady { side: 0, count: 0, max: 100 };
    let right = AlwaysReady { side: 1, count: 0, max: 100 };
    let mut merged = left.merge(right);
    let mut counts = [0u32; 2];
    for _ in 0..20 {
        if let Poll::Ready(Some(item)) = Pin::new(&mut merged).poll_next(&mut ctx) {
            counts[item as usize] += 1;
        }
    }
    eprintln!("counts after 20 polls: left={} right={}", counts[0], counts[1]);

    eprintln!("=== empty + never-ending merge ===");
    let empty = stream::empty::<i32>();
    let ne = stream::from_iter(0..3);
    let mut m = empty.merge(ne);
    for _ in 0..5 {
        match Pin::new(&mut m).poll_next(&mut ctx) {
            Poll::Ready(Some(i)) => eprintln!("  got {}", i),
            Poll::Ready(None) => { eprintln!("  got None"); break; }
            Poll::Pending => eprintln!("  pending"),
        }
    }

    eprintln!("=== empty + empty merge ===");
    let e1 = stream::empty::<i32>();
    let e2 = stream::empty::<i32>();
    let mut m = e1.merge(e2);
    for i in 0..5 {
        match Pin::new(&mut m).poll_next(&mut ctx) {
            Poll::Ready(Some(_)) => eprintln!("  poll {} got Some", i),
            Poll::Ready(None) => { eprintln!("  poll {} got None", i); break; }
            Poll::Pending => eprintln!("  poll {} pending", i),
        }
    }
}