use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use pin_project_lite::pin_project;

use crate::stream::Stream;
use crate::utils::{timer_at, Timer};

#[cfg(feature = "unstable")]
fn instant_add_saturating(instant: Instant, duration: Duration) -> Instant {
    instant.checked_add(duration).unwrap_or_else(|| {
        Instant::now().checked_add(Duration::from_secs(86400 * 365 * 100)).unwrap_or(instant)
    })
}

#[cfg(feature = "unstable")]
fn saturating_advance_tick(current: Instant, interval: Duration, now: Instant) -> Instant {
    if interval == Duration::ZERO {
        return now;
    }

    if current > now {
        return current;
    }

    let nanos_per_tick = interval.as_nanos() as u128;
    let nanos_elapsed = now.saturating_duration_since(current).as_nanos() as u128;

    if nanos_per_tick == 0 || nanos_elapsed == 0 {
        return instant_add_saturating(current, interval);
    }

    let ticks_to_advance = (nanos_elapsed / nanos_per_tick).saturating_add(1);
    let max_safe_ticks = u64::MAX as u128 / nanos_per_tick;

    let actual_ticks = if ticks_to_advance > max_safe_ticks {
        max_safe_ticks
    } else {
        ticks_to_advance
    };

    let advance_duration = Duration::from_nanos((actual_ticks * nanos_per_tick) as u64);
    instant_add_saturating(current, advance_duration)
}

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
/// Missed ticks are skipped rather than caught up: if the consumer is slow and
/// multiple intervals pass between polls, only one item will be yielded and
/// the stream will skip ahead to the next future tick.
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
    let next_tick = if dur == Duration::ZERO {
        start
    } else {
        instant_add_saturating(start, dur)
    };
    Interval {
        start,
        interval: dur,
        next_tick,
        delay: None,
        has_yielded: false,
    }
}

pin_project! {
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
        start: Instant,
        interval: Duration,
        next_tick: Instant,
        #[pin]
        delay: Option<Timer>,
        has_yielded: bool,
    }
}

impl Stream for Interval {
    type Item = ();

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let now = Instant::now();
            let mut this = self.as_mut().project();

            if now < *this.next_tick {
                if this.delay.is_none() {
                    this.delay.set(Some(timer_at(*this.next_tick)));
                }

                let poll_result = match this.delay.as_mut().as_pin_mut() {
                    Some(d) => d.poll(cx),
                    None => Poll::Pending,
                };

                if poll_result.is_pending() {
                    return Poll::Pending;
                }

                continue;
            }

            if this.interval == &Duration::ZERO {
                *this.next_tick = instant_add_saturating(now, Duration::from_nanos(1));
                this.delay.set(Some(timer_at(*this.next_tick)));
                *this.has_yielded = true;
                return Poll::Ready(Some(()));
            }

            if !*this.has_yielded {
                if *this.next_tick < now {
                    *this.next_tick = saturating_advance_tick(*this.next_tick, *this.interval, now);
                    this.delay.set(None);
                    *this.has_yielded = true;
                    continue;
                }
                *this.has_yielded = true;
            }

            *this.has_yielded = true;
            let result = Poll::Ready(Some(()));

            *this.next_tick = saturating_advance_tick(*this.next_tick, *this.interval, now);

            this.delay.set(None);

            return result;
        }
    }
}
