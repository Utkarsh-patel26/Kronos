package io.kronos.raft.election;

import io.kronos.common.model.NodeId;

/**
 * Counts votes for a single election round. Not thread-safe — only accessed
 * from the raft thread, where all vote responses are delivered.
 */
public final class VoteTracker {

    private final int majority;
    private int granted = 0;
    private int denied  = 0;

    public VoteTracker(int clusterSize) {
        this.majority = (clusterSize / 2) + 1;
    }

    public void recordVote(NodeId peer, boolean voteGranted) {
        if (voteGranted) granted++;
        else             denied++;
    }

    public boolean hasWon()  { return granted >= majority; }
    public boolean hasLost() { return denied  >= majority; }
}
