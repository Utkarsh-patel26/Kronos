package io.kronos.raft.log;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;

import java.util.ArrayList;
import java.util.List;

/**
 * In-memory Raft log. Not thread-safe — must only be accessed from the raft thread.
 * Phase 5 will replace this with a WAL-backed implementation.
 */
public final class InMemoryRaftLog implements RaftLog {

    private static final LogEntry SENTINEL = new LogEntry(Term.ZERO, LogIndex.ZERO, new byte[0]);

    private final List<LogEntry> entries = new ArrayList<>();

    @Override
    public LogEntry lastEntry() {
        if (entries.isEmpty()) return SENTINEL;
        return entries.get(entries.size() - 1);
    }

    @Override
    public LogEntry entryAt(LogIndex index) {
        if (index.value() == 0) return SENTINEL;
        int i = (int) (index.value() - 1);
        if (i >= entries.size()) {
            throw new IndexOutOfBoundsException("no entry at index " + index.value());
        }
        return entries.get(i);
    }

    @Override
    public void append(LogEntry entry) {
        entries.add(entry);
    }

    @Override
    public List<LogEntry> entriesFrom(LogIndex from) {
        if (from.value() == 0) return List.copyOf(entries);
        int i = (int) (from.value() - 1);
        if (i >= entries.size()) return List.of();
        return List.copyOf(entries.subList(i, entries.size()));
    }

    @Override
    public void truncateFrom(LogIndex from) {
        if (from.value() == 0) {
            entries.clear();
            return;
        }
        int i = (int) (from.value() - 1);
        if (i < entries.size()) {
            entries.subList(i, entries.size()).clear();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }
}
