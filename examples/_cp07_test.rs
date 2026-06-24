use std::time::Duration;

use async_std::stream::interval;
use async_std::task;

fn main() {
    // CP-07: drop without poll, then construct+drop many in a loop
    let mut handles = vec![];
    for _ in 0..5 {
        handles.push(std::thread::spawn(|| {
            task::block_on(async {
                for _ in 0..2000 {
                    let _i = interval(Duration::from_secs(3600));
                }
            });
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    eprintln!("CP-07 passed: created and dropped 10000 intervals without panic");
}