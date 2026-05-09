package io.kronos.raft.election;

import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Randomised election timeout. When reset() is called the current pending timeout
 * is cancelled and a fresh one scheduled with a delay drawn uniformly from
 * [minMs, maxMs). The onTimeout callback fires on the supplied scheduler thread.
 */
public final class ElectionTimer {

    private final int minMs;
    private final int maxMs;
    private final Random rng = new Random();
    private final ScheduledExecutorService scheduler;
    private final Runnable onTimeout;
    private ScheduledFuture<?> scheduled;

    public ElectionTimer(int minMs, int maxMs,
                         ScheduledExecutorService scheduler, Runnable onTimeout) {
        this.minMs     = minMs;
        this.maxMs     = maxMs;
        this.scheduler = scheduler;
        this.onTimeout = onTimeout;
    }

    public void reset() {
        if (scheduled != null) scheduled.cancel(false);
        int delay = minMs + rng.nextInt(maxMs - minMs);
        scheduled = scheduler.schedule(onTimeout, delay, TimeUnit.MILLISECONDS);
    }

    public void stop() {
        if (scheduled != null) {
            scheduled.cancel(false);
            scheduled = null;
        }
    }
}
