package io.kronos.raft.log;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;
import io.kronos.storage.wal.WriteAheadLog;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * WAL-backed {@link RaftLog}. In-memory list mirrors the WAL for O(1) access;
 * the WAL provides durability across restarts.
 *
 * <p>Entries whose index is at or below {@code snapshotBase.index()} have been
 * compacted into a snapshot and are no longer stored in memory. The snapshot base
 * entry acts as a sentinel for the log-consistency checks.
 *
 * <p>Not thread-safe — must only be accessed from the Raft thread.
 */
public final class PersistentRaftLog implements RaftLog {

    private static final LogEntry SENTINEL = new LogEntry(Term.ZERO, LogIndex.ZERO, new byte[0]);

    private final WriteAheadLog wal;
    private final List<LogEntry> entries = new ArrayList<>();
    /** The snapshot boundary: entries at or below this index have been compacted. */
    private LogEntry snapshotBase;

    /** Construct with no prior snapshot (fresh node). */
    public PersistentRaftLog(WriteAheadLog wal) {
        this(wal, null);
    }

    /**
     * Construct with a known snapshot base (used when restarting a node that has
     * taken at least one snapshot). WAL entries at or below {@code initialSnapshotBase}
     * are skipped during recovery.
     */
    public PersistentRaftLog(WriteAheadLog wal, LogEntry initialSnapshotBase) {
        this.wal          = wal;
        this.snapshotBase = initialSnapshotBase != null ? initialSnapshotBase : SENTINEL;
        recover();
    }

    private void recover() {
        try {
            long snapIdx = snapshotBase.index().value();
            for (LogEntry e : wal.recover()) {
                if (e.index().value() > snapIdx) {
                    entries.add(e);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException("WAL recovery failed", e);
        }
    }

    @Override
    public LogEntry lastEntry() {
        return entries.isEmpty() ? snapshotBase : entries.get(entries.size() - 1);
    }

    @Override
    public LogEntry entryAt(LogIndex index) {
        if (index.value() == 0) return SENTINEL;
        if (index.equals(snapshotBase.index())) return snapshotBase;
        long snapIdx = snapshotBase.index().value();
        long offset  = index.value() - snapIdx - 1;
        if (offset < 0 || offset >= entries.size()) {
            throw new IndexOutOfBoundsException("no entry at index " + index.value()
                + " (snapshot base=" + snapIdx + ", in-memory entries=" + entries.size() + ")");
        }
        return entries.get((int) offset);
    }

    @Override
    public void append(LogEntry entry) {
        try {
            wal.persist(entry);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL persist failed", e);
        }
        entries.add(entry);
    }

    @Override
    public List<LogEntry> entriesFrom(LogIndex from) {
        long snapIdx = snapshotBase.index().value();
        if (from.value() <= snapIdx) return List.copyOf(entries);
        int offset = (int) (from.value() - snapIdx - 1);
        if (offset >= entries.size()) return List.of();
        return List.copyOf(entries.subList(offset, entries.size()));
    }

    @Override
    public void truncateFrom(LogIndex from) {
        try {
            wal.truncateFrom(from);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL truncate failed", e);
        }
        long snapIdx = snapshotBase.index().value();
        if (from.value() <= snapIdx) {
            entries.clear();
            return;
        }
        int offset = (int) (from.value() - snapIdx - 1);
        if (offset < entries.size()) {
            entries.subList(offset, entries.size()).clear();
        }
    }

    @Override
    public int size() {
        return entries.size();
    }

    @Override
    public LogIndex snapshotIndex() {
        return snapshotBase.index();
    }

    @Override
    public void resetToSnapshot(LogIndex lastIndex, Term lastTerm) {
        snapshotBase = new LogEntry(lastTerm, lastIndex, new byte[0]);
        entries.clear();
        try {
            wal.deleteSegmentsBefore(lastIndex);
        } catch (IOException e) {
            throw new UncheckedIOException("WAL compaction failed", e);
        }
    }
}
