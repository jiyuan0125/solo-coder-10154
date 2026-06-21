/// Calls a function and aborts if it panics.
///
/// This is useful in unsafe code where we can't recover from panics.
#[cfg(feature = "default")]
#[inline]
pub fn abort_on_panic<T>(f: impl FnOnce() -> T) -> T {
    struct Bomb;

    impl Drop for Bomb {
        fn drop(&mut self) {
            std::process::abort();
        }
    }

    let bomb = Bomb;
    let t = f();
    std::mem::forget(bomb);
    t
}

/// Add additional context to errors
#[cfg(feature = "std")]
pub(crate) trait Context {
    fn context(self, message: impl Fn() -> String) -> Self;
}

#[cfg(all(
    not(target_os = "unknown"),
    any(feature = "default", feature = "unstable")
))]
mod timer {
    pub type Timer = async_io::Timer;
}

#[cfg(any(feature = "unstable", feature = "default"))]
pub(crate) fn timer_after(dur: std::time::Duration) -> timer::Timer {
    Timer::after(dur)
}

#[cfg(feature = "unstable")]
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn timer_at(at: std::time::Instant) -> timer::Timer {
    Timer::at(at)
}

#[cfg(feature = "unstable")]
#[cfg(target_arch = "wasm32")]
pub(crate) fn timer_at(at: std::time::Instant) -> timer::Timer {
    let now = std::time::Instant::now();
    let dur = if at > now { at - now } else { std::time::Duration::ZERO };
    Timer::after(dur)
}

#[cfg(any(all(target_arch = "wasm32", feature = "default"),))]
mod timer {
    use std::pin::Pin;
    use std::task::Poll;

    use gloo_timers::future::TimeoutFuture;

    #[derive(Debug)]
    pub(crate) struct Timer(TimeoutFuture);

    impl Timer {
        pub(crate) fn after(dur: std::time::Duration) -> Self {
            // Round up to the nearest millisecond.
            let mut timeout_ms = dur.as_millis() as u32;
            if std::time::Duration::from_millis(timeout_ms as u64) < dur {
                timeout_ms += 1;
            }

            Timer(TimeoutFuture::new(timeout_ms))
        }
    }

    impl std::future::Future for Timer {
        type Output = ();

        fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
            match Pin::new(&mut self.0).poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(_) => Poll::Ready(()),
            }
        }
    }
}

#[cfg(any(feature = "unstable", feature = "default"))]
pub(crate) use timer::*;

/// Defers evaluation of a block of code until the end of the scope.
#[cfg(feature = "default")]
#[doc(hidden)]
macro_rules! defer {
    ($($body:tt)*) => {
        let _guard = {
            pub struct Guard<F: FnOnce()>(Option<F>);

            impl<F: FnOnce()> Drop for Guard<F> {
                fn drop(&mut self) {
                    (self.0).take().map(|f| f());
                }
            }

            Guard(Some(|| {
                let _ = { $($body)* };
            }))
        };
    };
}

/// Declares unstable items.
#[doc(hidden)]
macro_rules! cfg_unstable {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "unstable")]
            #[cfg_attr(feature = "docs", doc(cfg(unstable)))]
            $item
        )*
    }
}

/// Declares unstable and default items.
#[doc(hidden)]
macro_rules! cfg_unstable_default {
    ($($item:item)*) => {
        $(
            #[cfg(all(feature = "default", feature = "unstable"))]
            #[cfg_attr(feature = "docs", doc(cfg(unstable)))]
            $item
        )*
    }
}

/// Declares Unix-specific items.
#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! cfg_unix {
    ($($item:item)*) => {
        $(
            #[cfg(any(unix, feature = "docs"))]
            $item
        )*
    }
}

/// Declares Windows-specific items.
#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! cfg_windows {
    ($($item:item)*) => {
        $(
            #[cfg(any(windows, feature = "docs"))]
            $item
        )*
    }
}

/// Declares items when the "docs" feature is enabled.
#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! cfg_docs {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "docs")]
            $item
        )*
    }
}

/// Declares items when the "docs" feature is disabled.
#[doc(hidden)]
#[allow(unused_macros)]
macro_rules! cfg_not_docs {
    ($($item:item)*) => {
        $(
            #[cfg(not(feature = "docs"))]
            $item
        )*
    }
}

/// Declares std items.
#[allow(unused_macros)]
#[doc(hidden)]
macro_rules! cfg_std {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "std")]
            $item
        )*
    }
}

/// Declares no-std items.
#[allow(unused_macros)]
#[doc(hidden)]
macro_rules! cfg_alloc {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "alloc")]
            $item
        )*
    }
}

/// Declares default items.
#[allow(unused_macros)]
#[doc(hidden)]
macro_rules! cfg_default {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "default")]
            $item
        )*
    }
}

/// Declares items that use I/O safety.
#[allow(unused_macros)]
#[doc(hidden)]
macro_rules! cfg_io_safety {
    ($($item:item)*) => {
        $(
            #[cfg(feature = "io_safety")]
            $item
        )*
    }
}
