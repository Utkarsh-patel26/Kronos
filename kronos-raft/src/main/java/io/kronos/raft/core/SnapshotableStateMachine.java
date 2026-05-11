package io.kronos.raft.core;

/**
 * A state machine that can participate in the Raft snapshot protocol.
 * Implementations must produce a deterministic byte serialization of their state.
 */
public interface SnapshotableStateMachine extends StateMachine {

    /** Serialize the current state to bytes. Called on the Raft thread. */
    byte[] takeSnapshot();

    /** Replace the current state with the bytes from a snapshot. Called on the Raft thread. */
    void installSnapshot(byte[] snapshot);
}
