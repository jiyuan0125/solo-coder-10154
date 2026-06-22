use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use crate::stream::Stream;
use crate::utils::{timer_at, Timer};

/// Creates a new stream that yields at a set interval.
///
/// The stream first yields after `dur`, and continues to yield every
/// `dur` after that. The stream accounts for time elapsed between calls, and
/// will adjust accordingly to prevent time skews.
///
/// Each interval may be slightly longer than the specified duration, but never
/// less.
///
/// Note that intervals are not intended for high resolution timers, but rather
/// they will likely fire some granularity after the exact instant that they're
/// otherwise indicated to fire at.
///
/// See also: [`task::sleep`].
///
/// [`task::sleep`]: ../task/fn.sleep.html
///
/// # Examples
///
/// Basic example:
///
/// ```no_run
/// use async_std::prelude::*;
/// use async_std::stream;
/// use std::time::Duration;
///
/// # fn main() -> std::io::Result<()> { async_std::task::block_on(async {
/// #
/// let mut interval = stream::interval(Duration::from_secs(4));
/// while let Some(_) = interval.next().await {
///     println!("prints every four seconds");
/// }
/// #
/// # Ok(()) }) }
/// ```
#[cfg(feature = "unstable")]
#[cfg_attr(feature = "docs", doc(cfg(unstable)))]
pub fn interval(dur: Duration) -> Interval {
    let start = Instant::now();
    let tick_count = 0u64;
    let deadline = match next_deadline(start, dur, tick_count + 1) {
        Some(d) => d,
        None => Instant::now(),
    };
    Interval {
        delay: timer_at(deadline),
        interval: dur,
        start,
        tick_count,
        exhausted: false,
    }
}

/// A stream representing notifications at fixed interval
///
/// This stream is created by the [`interval`] function. See its
/// documentation for more.
///
/// [`interval`]: fn.interval.html
#[cfg(feature = "unstable")]
#[cfg_attr(feature = "docs", doc(cfg(unstable)))]
#[derive(Debug)]
pub struct Interval {
    delay: Timer,
    interval: Duration,
    start: Instant,
    tick_count: u64,
    exhausted: bool,
}

#[inline]
fn mul_by_2_pow_32(d: Duration) -> Option<Duration> {
    let mut result = d;
    for _ in 0..32 {
        result = result.checked_add(result)?;
    }
    Some(result)
}

#[inline]
fn checked_mul_duration(interval: Duration, n: u64) -> Option<Duration> {
    if n == 0 {
        return Some(Duration::ZERO);
    }
    if n <= u32::MAX as u64 {
        return interval.checked_mul(n as u32);
    }
    let high = (n >> 32) as u32;
    let low = n as u32;
    let part_high = mul_by_2_pow_32(interval.checked_mul(high)?)?;
    let part_low = interval.checked_mul(low)?;
    part_high.checked_add(part_low)
}

#[inline]
fn next_deadline(start: Instant, interval: Duration, n: u64) -> Option<Instant> {
    let multiplied = checked_mul_duration(interval, n)?;
    start.checked_add(multiplied)
}

impl Stream for Interval {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if Pin::new(&mut self.delay).poll(cx).is_pending() {
            return Poll::Pending;
        }
        if self.exhausted {
            return Poll::Ready(None);
        }
        self.tick_count += 1;
        match next_deadline(self.start, self.interval, self.tick_count + 1) {
            Some(deadline) => {
                let _ = std::mem::replace(&mut self.delay, timer_at(deadline));
            }
            None => {
                self.exhausted = true;
                let _ = std::mem::replace(&mut self.delay, timer_at(Instant::now()));
            }
        }
        Poll::Ready(Some(()))
    }
}
