package io.kronos.common.config;

import io.kronos.common.model.NodeId;

import java.util.Objects;

/**
 * Address of another cluster member. The pair (host, raftPort) identifies
 * the socket used for internal Raft RPCs; the nodeId is the peer's stable
 * logical identity.
 */
public record PeerConfig(NodeId nodeId, String host, int raftPort) {

    public PeerConfig {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(host, "host");
        if (host.isBlank()) {
            throw new IllegalArgumentException("peer host must not be blank");
        }
        if (raftPort <= 0 || raftPort > 65_535) {
            throw new IllegalArgumentException("peer raftPort out of range: " + raftPort);
        }
    }

    @Override
    public String toString() {
        return nodeId + "@" + host + ":" + raftPort;
    }
}
