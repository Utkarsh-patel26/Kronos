package io.kronos.raft.core;

import io.kronos.common.model.LogIndex;

/**
 * Applied to each committed log entry in index order, exactly once.
 * Implementations must be deterministic — all nodes applying the same
 * sequence of entries must reach identical state.
 */
@FunctionalInterface
public interface StateMachine {
    void apply(LogIndex index, byte[] command);
}
