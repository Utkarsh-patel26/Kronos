package io.kronos.raft.core;

import io.kronos.common.model.NodeId;

/**
 * Thrown when a write is submitted to a node that is not the current leader.
 */
public final class NotLeaderException extends RuntimeException {

    private final NodeId knownLeader;

    public NotLeaderException(NodeId knownLeader) {
        super(knownLeader != null
            ? "not leader; known leader is " + knownLeader.value()
            : "not leader; no known leader");
        this.knownLeader = knownLeader;
    }

    /** The leader this node last heard from, or null if unknown. */
    public NodeId knownLeader() { return knownLeader; }
}
