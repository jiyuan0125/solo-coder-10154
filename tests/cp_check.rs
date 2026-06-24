// Test for CP-08/CP-09/CP-10/CP-11 - task::spawn failure path
// Since async_global_executor::spawn is infallible, we can't trigger failure directly.
// We can only verify the public API contract.

use std::io;
use std::time::Duration;
use async_std::prelude::*;
use async_std::task;

#[test]
fn cp08_spawn_output_type() {
    fn assert_output_is_result_t_io_error<T>(h: task::JoinHandle<T>)
    where
        T: Send + 'static,
    {
        fn _check<F: std::future::Future<Output = Result<T, io::Error>>, T>() {}
        _check::<std::pin::Pin<Box<task::JoinHandle<T>>>>();
    }

    let h = task::spawn(async { 42i32 });
    // Compile-time check that .await returns Result<T, io::Error>
    let _check: fn(task::JoinHandle<i32>) -> _ = |h| async move {
        let _: Result<i32, io::Error> = h.await;
    };
    drop(h);
}

#[test]
fn cp09_spawn_local_output_type() {
    task::block_on(async {
        let h = task::spawn_local(async { 42i32 });
        let _: Result<i32, io::Error> = h.await;
    });
}

#[test]
fn cp09_spawn_blocking_output_type() {
    let res: i32 = task::spawn_blocking(|| 42).await;
    assert_eq!(res, 42);
}

#[test]
fn cp10_joinhandle_methods_safe() {
    task::block_on(async {
        let h = task::spawn(async { 42i32 });
        // Call task() - safe
        let t = h.task();
        let _id = t.id();
        // await - returns Result<i32, io::Error>
        let result = h.await;
        assert_eq!(result.unwrap(), 42);
    });
}

#[test]
fn cp11_builder_spawn_returns_result() {
    let builder = task::Builder::new();
    let h = builder.name("test".to_string()).spawn(async { 42i32 });
    assert!(h.is_ok());
    task::block_on(async {
        let r: i32 = h.unwrap().await;
        assert_eq!(r, 42);
    });
}

#[test]
fn cp07_drop_interval_no_panic() {
    // Create a large number of intervals and drop them without polling
    for _ in 0..100 {
        let _i = async_std::stream::interval(Duration::from_secs(3600));
    }
}