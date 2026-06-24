use async_std::prelude::*;
use async_std::stream;
use async_std::task;
use std::time::{Duration, Instant};

fn main() {
    task::block_on(async {
        // ====== CP-01: delayed first poll ======
        let start = Instant::now();
        let mut interval = stream::interval(Duration::from_millis(100));
        task::sleep(Duration::from_millis(500)).await;
        let first = interval.next().await;
        let elapsed = start.elapsed();
        println!("CP-01: first yield after {}ms, expected >= 100ms", elapsed.as_millis());
        let next_five_elapsed: Vec<_> = (0..5).map(|i| {
            let t0 = Instant::now();
            (i, t0)
        }).collect();
        // For CP-01 we mainly need first poll > 100ms
        // Then quickly poll 5 more to ensure they don't fire all 5 back-to-back
        let mut times = vec![];
        times.push(start.elapsed());
        for _ in 0..5 {
            interval.next().await;
            times.push(start.elapsed());
        }
        for (i, t) in times.iter().enumerate() {
            println!("  yield #{}: {}ms", i, t.as_millis());
        }
        let last = *times.last().unwrap();
        let first = *times.first().unwrap();
        let avg_gap_ms = (last.as_millis() - first.as_millis()) / 5;
        println!("CP-01: avg gap over 6 yields = {}ms (should be ~100ms not 1ms)", avg_gap_ms);

        // ====== CP-02: slow consumer - 1ms interval, sleep 10s before each poll ======
        let start2 = Instant::now();
        let mut interval2 = stream::interval(Duration::from_millis(1));
        let mut gaps = vec![];
        for i in 0..5 {
            task::sleep(Duration::from_secs(10)).await;
            let t_poll = Instant::now();
            interval2.next().await;
            let gap = t_poll.elapsed();
            gaps.push(gap);
            println!("CP-02: poll #{} gap = {}ms (should be tens of ms, not 0ms)", i, gap.as_millis());
        }
        let total = start2.elapsed().as_millis();
        println!("CP-02: total time = {}ms (should be ~50000ms)", total);

        // ====== CP-03: drift over 200 ticks @ 50ms ======
        let start3 = Instant::now();
        let mut interval3 = stream::interval(Duration::from_millis(50));
        let mut timestamps = vec![];
        for i in 0..200 {
            interval3.next().await;
            timestamps.push(start3.elapsed());
        }
        let mut gaps = vec![];
        for w in timestamps.windows(2) {
            gaps.push((w[1] - w[0]).as_millis());
        }
        let avg_gap = gaps.iter().sum::<u128>() / gaps.len() as u128;
        let last = timestamps.last().unwrap().as_millis();
        println!("CP-03: avg gap = {}ms (should be ~50ms), last tick = {}ms (should be ~10000ms)", avg_gap, last);

        // ====== CP-04: huge duration ======
        let huge = Duration::from_secs(u64::MAX / 1_000_000);
        println!("CP-04: huge duration = {}s", huge.as_secs());
        let _interval4 = stream::interval(huge);
        // Just need this to construct without panic.
        // Then test first poll - this is unbounded so don't actually await
        // But test more modest boundary
        let bigish = Duration::from_secs(3600 * 24 * 365 * 100);
        let _interval4b = stream::interval(bigish);
        println!("CP-04: large duration constructed OK");
    });
}
