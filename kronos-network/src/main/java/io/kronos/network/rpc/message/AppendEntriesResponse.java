package io.kronos.network.rpc.message;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;

import java.util.Objects;

public final class AppendEntriesResponse {

    private final Term     term;
    private final boolean  success;
    private final LogIndex matchIndex; // valid only when success=true

    public AppendEntriesResponse(Term term, boolean success, LogIndex matchIndex) {
        this.term       = Objects.requireNonNull(term, "term");
        this.success    = success;
        this.matchIndex = Objects.requireNonNull(matchIndex, "matchIndex");
    }

    public Term     term()       { return term; }
    public boolean  success()    { return success; }
    public LogIndex matchIndex() { return matchIndex; }

    @Override
    public String toString() {
        return "AppendEntriesResponse{term=" + term + ", success=" + success
            + ", matchIndex=" + matchIndex + "}";
    }
}
