package io.kronos.network.rpc.message;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;

import java.util.Arrays;
import java.util.Objects;

public final class InstallSnapshotRequest {

    private final Term     term;
    private final NodeId   leaderId;
    private final LogIndex lastIncludedIndex;
    private final Term     lastIncludedTerm;
    private final int      offset;
    private final byte[]   data;
    private final boolean  done;

    public InstallSnapshotRequest(Term term, NodeId leaderId,
                                  LogIndex lastIncludedIndex, Term lastIncludedTerm,
                                  int offset, byte[] data, boolean done) {
        this.term              = Objects.requireNonNull(term, "term");
        this.leaderId          = Objects.requireNonNull(leaderId, "leaderId");
        this.lastIncludedIndex = Objects.requireNonNull(lastIncludedIndex, "lastIncludedIndex");
        this.lastIncludedTerm  = Objects.requireNonNull(lastIncludedTerm, "lastIncludedTerm");
        if (offset < 0) throw new IllegalArgumentException("offset < 0");
        this.offset            = offset;
        Objects.requireNonNull(data, "data");
        this.data              = data.clone();
        this.done              = done;
    }

    public Term     term()              { return term; }
    public NodeId   leaderId()          { return leaderId; }
    public LogIndex lastIncludedIndex() { return lastIncludedIndex; }
    public Term     lastIncludedTerm()  { return lastIncludedTerm; }
    public int      offset()            { return offset; }
    public byte[]   data()              { return data.clone(); }
    public boolean  done()              { return done; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstallSnapshotRequest r)) return false;
        return offset == r.offset && done == r.done
            && term.equals(r.term) && leaderId.equals(r.leaderId)
            && lastIncludedIndex.equals(r.lastIncludedIndex)
            && lastIncludedTerm.equals(r.lastIncludedTerm)
            && Arrays.equals(data, r.data);
    }

    @Override
    public int hashCode() {
        int h = Objects.hash(term, leaderId, lastIncludedIndex, lastIncludedTerm, offset, done);
        return 31 * h + Arrays.hashCode(data);
    }

    @Override
    public String toString() {
        return "InstallSnapshotRequest{term=" + term + ", leader=" + leaderId.value()
            + ", lastIndex=" + lastIncludedIndex + ", offset=" + offset
            + ", dataLen=" + data.length + ", done=" + done + "}";
    }
}
