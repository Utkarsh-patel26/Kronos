package io.kronos.raft.log;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;

import java.util.List;

public interface RaftLog {
    /** Last entry, or a sentinel with term=ZERO and index=ZERO if the log is empty. */
    LogEntry lastEntry();

    /** Entry at the given 1-based index, or the sentinel if index is ZERO. */
    LogEntry entryAt(LogIndex index);

    void append(LogEntry entry);

    /** All entries at index >= from (inclusive). Returns an empty list if none exist. */
    List<LogEntry> entriesFrom(LogIndex from);

    /** Remove all entries with index >= from (used for conflict resolution). */
    void truncateFrom(LogIndex from);

    int size();
}
