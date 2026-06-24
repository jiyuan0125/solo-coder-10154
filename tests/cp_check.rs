// Test for CP-08/CP-09/CP-10/CP-11 - task::spawn failure path
use std::io;
use std::time::Duration;
use async_std::task;

#[test]
fn cp09_spawn_local_output_type() {
    task::block_on(async {
        let h = task::spawn_local(async { 42i32 });
        let _: Result<i32, io::Error> = h.await;
    });
}

#[test]
fn cp09_spawn_blocking_output_type() {
    let res: Result<i32, io::Error> = task::block_on(async {
        task::spawn_blocking(|| 42i32).await
    });
    assert_eq!(res.unwrap(), 42);
}

#[test]
fn cp10_joinhandle_methods_safe() {
    task::block_on(async {
        let h = task::spawn(async { 42i32 });
        let t = h.task();
        let _id = t.id();
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
        let r: Result<i32, io::Error> = h.unwrap().await;
        assert_eq!(r.unwrap(), 42);
    });
}

#[test]
fn cp07_drop_interval_no_panic() {
    for _ in 0..100 {
        let _i = async_std::stream::interval(Duration::from_secs(3600));
    }
}