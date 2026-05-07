package io.kronos.network.rpc.message;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class AppendEntriesRequest {

    private final Term          term;
    private final NodeId        leaderId;
    private final LogIndex      prevLogIndex;
    private final Term          prevLogTerm;
    private final List<LogEntry> entries;
    private final LogIndex      leaderCommit;

    public AppendEntriesRequest(Term term, NodeId leaderId,
                                LogIndex prevLogIndex, Term prevLogTerm,
                                List<LogEntry> entries, LogIndex leaderCommit) {
        this.term         = Objects.requireNonNull(term, "term");
        this.leaderId     = Objects.requireNonNull(leaderId, "leaderId");
        this.prevLogIndex = Objects.requireNonNull(prevLogIndex, "prevLogIndex");
        this.prevLogTerm  = Objects.requireNonNull(prevLogTerm, "prevLogTerm");
        Objects.requireNonNull(entries, "entries");
        this.entries      = Collections.unmodifiableList(new ArrayList<>(entries));
        this.leaderCommit = Objects.requireNonNull(leaderCommit, "leaderCommit");
    }

    public Term          term()         { return term; }
    public NodeId        leaderId()     { return leaderId; }
    public LogIndex      prevLogIndex() { return prevLogIndex; }
    public Term          prevLogTerm()  { return prevLogTerm; }
    public List<LogEntry> entries()     { return entries; }
    public LogIndex      leaderCommit() { return leaderCommit; }

    /** Convenience — true when this is a heartbeat (no entries). */
    public boolean isHeartbeat() { return entries.isEmpty(); }

    @Override
    public String toString() {
        return "AppendEntriesRequest{term=" + term + ", leader=" + leaderId.value()
            + ", prevLogIndex=" + prevLogIndex + ", prevLogTerm=" + prevLogTerm
            + ", entries=" + entries.size() + ", leaderCommit=" + leaderCommit + "}";
    }
}
