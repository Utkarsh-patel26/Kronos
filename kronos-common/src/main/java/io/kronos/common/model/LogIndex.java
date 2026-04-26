package io.kronos.common.model;

/**
 * A position in the replicated log. Indexes start at 1; {@link #ZERO} is
 * reserved to mean "before the first entry" (used for prev-log pointers in
 * AppendEntries RPCs when the log is empty).
 */
public record LogIndex(long value) implements Comparable<LogIndex> {

    public static final LogIndex ZERO = new LogIndex(0);
    public static final LogIndex ONE = new LogIndex(1);

    public LogIndex {
        if (value < 0) {
            throw new IllegalArgumentException("LogIndex must be non-negative, got " + value);
        }
    }

    public static LogIndex of(long value) {
        return new LogIndex(value);
    }

    public LogIndex increment() {
        return new LogIndex(value + 1);
    }

    public LogIndex decrement() {
        if (value == 0) {
            throw new IllegalStateException("Cannot decrement LogIndex.ZERO");
        }
        return new LogIndex(value - 1);
    }

    public LogIndex plus(long delta) {
        return new LogIndex(value + delta);
    }

    public boolean isGreaterThan(LogIndex other) {
        return compareTo(other) > 0;
    }

    public boolean isLessThan(LogIndex other) {
        return compareTo(other) < 0;
    }

    @Override
    public int compareTo(LogIndex other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public String toString() {
        return "#" + value;
    }
}
