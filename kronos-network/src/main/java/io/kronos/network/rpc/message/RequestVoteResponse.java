package io.kronos.network.rpc.message;

import io.kronos.common.model.Term;

import java.util.Objects;

public final class RequestVoteResponse {

    private final Term    term;
    private final boolean voteGranted;

    public RequestVoteResponse(Term term, boolean voteGranted) {
        this.term        = Objects.requireNonNull(term, "term");
        this.voteGranted = voteGranted;
    }

    public Term    term()        { return term; }
    public boolean voteGranted() { return voteGranted; }

    @Override
    public String toString() {
        return "RequestVoteResponse{term=" + term + ", voteGranted=" + voteGranted + "}";
    }
}
