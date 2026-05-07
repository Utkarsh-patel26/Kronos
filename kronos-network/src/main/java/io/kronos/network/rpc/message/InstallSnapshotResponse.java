package io.kronos.network.rpc.message;

import io.kronos.common.model.Term;

import java.util.Objects;

public final class InstallSnapshotResponse {

    private final Term term;

    public InstallSnapshotResponse(Term term) {
        this.term = Objects.requireNonNull(term, "term");
    }

    public Term term() { return term; }

    @Override
    public String toString() {
        return "InstallSnapshotResponse{term=" + term + "}";
    }
}
