package io.kronos.common.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * A single entry in the replicated log.
 *
 * <p>An entry binds a term and an index to an opaque byte payload. The
 * payload is the serialized state-machine command (e.g. a PUT or DELETE);
 * its exact shape is defined by the state-machine layer, not by Raft.
 */
public final class LogEntry {

    private final Term term;
    private final LogIndex index;
    private final byte[] payload;

    public LogEntry(Term term, LogIndex index, byte[] payload) {
        this.term = Objects.requireNonNull(term, "term");
        this.index = Objects.requireNonNull(index, "index");
        Objects.requireNonNull(payload, "payload");
        this.payload = payload.clone();
    }

    public Term term() {
        return term;
    }

    public LogIndex index() {
        return index;
    }

    /** Defensive copy — callers cannot mutate internal state. */
    public byte[] payload() {
        return payload.clone();
    }

    public int payloadLength() {
        return payload.length;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof LogEntry other)) return false;
        return term.equals(other.term)
            && index.equals(other.index)
            && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(term, index);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "LogEntry{" + term + ", " + index + ", payload=" + payload.length + "B}";
    }
}
