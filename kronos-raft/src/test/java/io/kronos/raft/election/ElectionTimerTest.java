package io.kronos.raft.election;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

class ElectionTimerTest {

    private final ScheduledExecutorService scheduler =
        Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void tearDown() {
        scheduler.shutdownNow();
    }

    @Test
    void firesWithinConfiguredWindow() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        long start = System.currentTimeMillis();

        ElectionTimer timer = new ElectionTimer(50, 100, scheduler, latch::countDown);
        timer.reset();

        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(System.currentTimeMillis() - start).isLessThan(400);
    }

    @Test
    void resetCancelsPendingAndSchedulesFresh() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        ElectionTimer timer = new ElectionTimer(200, 300, scheduler, () -> {
            count.incrementAndGet();
            latch.countDown();
        });

        timer.reset();
        Thread.sleep(50);
        timer.reset(); // should cancel the first and schedule a new one

        assertThat(latch.await(600, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(count.get()).isEqualTo(1);
    }

    @Test
    void stopPreventsCallbackFromFiring() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();

        ElectionTimer timer = new ElectionTimer(50, 100, scheduler, count::incrementAndGet);
        timer.reset();
        timer.stop();

        Thread.sleep(250);
        assertThat(count.get()).isZero();
    }

    @Test
    void canBeResetAfterStop() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ElectionTimer timer = new ElectionTimer(50, 100, scheduler, latch::countDown);

        timer.reset();
        timer.stop();
        timer.reset(); // must fire again after being re-armed

        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
    }

    @Test
    void multipleResetsFireExactlyOnce() throws InterruptedException {
        AtomicInteger count = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(1);

        ElectionTimer timer = new ElectionTimer(100, 150, scheduler, () -> {
            count.incrementAndGet();
            latch.countDown();
        });

        for (int i = 0; i < 10; i++) timer.reset();

        assertThat(latch.await(500, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(count.get()).isEqualTo(1);
    }
}
