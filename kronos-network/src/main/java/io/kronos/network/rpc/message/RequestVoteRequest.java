package io.kronos.network.rpc.message;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;

import java.util.Objects;

public final class RequestVoteRequest {

    private final Term     term;
    private final NodeId   candidateId;
    private final LogIndex lastLogIndex;
    private final Term     lastLogTerm;

    public RequestVoteRequest(Term term, NodeId candidateId,
                              LogIndex lastLogIndex, Term lastLogTerm) {
        this.term         = Objects.requireNonNull(term, "term");
        this.candidateId  = Objects.requireNonNull(candidateId, "candidateId");
        this.lastLogIndex = Objects.requireNonNull(lastLogIndex, "lastLogIndex");
        this.lastLogTerm  = Objects.requireNonNull(lastLogTerm, "lastLogTerm");
    }

    public Term     term()         { return term; }
    public NodeId   candidateId()  { return candidateId; }
    public LogIndex lastLogIndex() { return lastLogIndex; }
    public Term     lastLogTerm()  { return lastLogTerm; }

    @Override
    public String toString() {
        return "RequestVoteRequest{term=" + term + ", candidate=" + candidateId.value()
            + ", lastLogIndex=" + lastLogIndex + ", lastLogTerm=" + lastLogTerm + "}";
    }
}
