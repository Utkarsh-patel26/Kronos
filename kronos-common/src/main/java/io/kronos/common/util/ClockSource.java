package io.kronos.common.util;

/**
 * Abstraction over wall-clock and monotonic time. Raft election timers and
 * heartbeat schedulers take a ClockSource rather than calling
 * {@link System} directly so tests can drive time deterministically.
 */
public interface ClockSource {

    /** Milliseconds since the Unix epoch. */
    long wallTimeMillis();

    /** Monotonic nanoseconds — safe for measuring elapsed intervals. */
    long monotonicNanos();

    /** Default implementation that delegates to {@link System}. */
    ClockSource SYSTEM = new ClockSource() {
        @Override public long wallTimeMillis()  { return System.currentTimeMillis(); }
        @Override public long monotonicNanos()  { return System.nanoTime(); }
    };
}
