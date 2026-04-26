package io.kronos.common.model;

/**
 * A Raft term number. Monotonically non-decreasing across the cluster; higher
 * terms always win over lower ones during elections and replication.
 */
public record Term(long value) implements Comparable<Term> {

    public static final Term ZERO = new Term(0);

    public Term {
        if (value < 0) {
            throw new IllegalArgumentException("Term must be non-negative, got " + value);
        }
    }

    public static Term of(long value) {
        return new Term(value);
    }

    public Term increment() {
        return new Term(value + 1);
    }

    public boolean isGreaterThan(Term other) {
        return compareTo(other) > 0;
    }

    public boolean isLessThan(Term other) {
        return compareTo(other) < 0;
    }

    @Override
    public int compareTo(Term other) {
        return Long.compare(this.value, other.value);
    }

    @Override
    public String toString() {
        return "T" + value;
    }
}
