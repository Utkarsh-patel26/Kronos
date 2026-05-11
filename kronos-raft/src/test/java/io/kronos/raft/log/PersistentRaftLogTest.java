package io.kronos.raft.log;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;
import io.kronos.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class PersistentRaftLogTest {

    @TempDir
    Path tmpDir;

    private static LogEntry entry(long term, long index) {
        return new LogEntry(Term.of(term), LogIndex.of(index), ("cmd-" + index).getBytes());
    }

    private PersistentRaftLog open(Path dir) throws IOException {
        return new PersistentRaftLog(new WriteAheadLog(dir));
    }

    // -----------------------------------------------------------------------
    // Empty log behaviour
    // -----------------------------------------------------------------------

    @Test
    void emptyLogReturnsZeroSentinel() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        assertThat(log.lastEntry().index()).isEqualTo(LogIndex.ZERO);
        assertThat(log.lastEntry().term()).isEqualTo(Term.ZERO);
        assertThat(log.size()).isZero();
    }

    @Test
    void entryAtZeroReturnsSentinel() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        LogEntry sentinel = log.entryAt(LogIndex.ZERO);
        assertThat(sentinel.index()).isEqualTo(LogIndex.ZERO);
        assertThat(sentinel.term()).isEqualTo(Term.ZERO);
    }

    // -----------------------------------------------------------------------
    // Append and access
    // -----------------------------------------------------------------------

    @Test
    void appendAndEntryAt() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(2, 3));

        assertThat(log.size()).isEqualTo(3);
        assertThat(log.entryAt(LogIndex.of(2)).term().value()).isEqualTo(1);
        assertThat(log.entryAt(LogIndex.of(3)).term().value()).isEqualTo(2);
    }

    @Test
    void entriesFrom() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        for (int i = 1; i <= 5; i++) log.append(entry(1, i));

        List<LogEntry> got = log.entriesFrom(LogIndex.of(3));
        assertThat(got).hasSize(3);
        assertThat(got.get(0).index().value()).isEqualTo(3);
        assertThat(got.get(2).index().value()).isEqualTo(5);
    }

    @Test
    void entriesFromZeroReturnsAll() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        assertThat(log.entriesFrom(LogIndex.ZERO)).hasSize(2);
    }

    @Test
    void entriesFromBeyondEndReturnsEmpty() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        log.append(entry(1, 1));
        assertThat(log.entriesFrom(LogIndex.of(99))).isEmpty();
    }

    // -----------------------------------------------------------------------
    // Truncation
    // -----------------------------------------------------------------------

    @Test
    void truncateFrom() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        for (int i = 1; i <= 4; i++) log.append(entry(1, i));
        log.truncateFrom(LogIndex.of(3));

        assertThat(log.size()).isEqualTo(2);
        assertThat(log.lastEntry().index().value()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // WAL durability — restart from same directory
    // -----------------------------------------------------------------------

    @Test
    void entriesSurviveRestart() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            PersistentRaftLog log = new PersistentRaftLog(wal);
            log.append(entry(1, 1));
            log.append(entry(1, 2));
            log.append(entry(2, 3));
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            PersistentRaftLog log2 = new PersistentRaftLog(wal2);
            assertThat(log2.size()).isEqualTo(3);
            assertThat(log2.entryAt(LogIndex.of(2)).term().value()).isEqualTo(1);
            assertThat(log2.entryAt(LogIndex.of(3)).term().value()).isEqualTo(2);
        }
    }

    @Test
    void truncationSurvivesRestart() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            PersistentRaftLog log = new PersistentRaftLog(wal);
            for (int i = 1; i <= 5; i++) log.append(entry(1, i));
            log.truncateFrom(LogIndex.of(4));
            log.append(entry(2, 4)); // append new entry after truncation
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            PersistentRaftLog log2 = new PersistentRaftLog(wal2);
            assertThat(log2.size()).isEqualTo(4);
            assertThat(log2.lastEntry().index().value()).isEqualTo(4);
            assertThat(log2.lastEntry().term().value()).isEqualTo(2);
        }
    }

    // -----------------------------------------------------------------------
    // Snapshot support
    // -----------------------------------------------------------------------

    @Test
    void snapshotIndexIsZeroInitially() throws IOException {
        PersistentRaftLog log = open(tmpDir);
        assertThat(log.snapshotIndex()).isEqualTo(LogIndex.ZERO);
    }

    @Test
    void resetToSnapshotClearsEntries() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            PersistentRaftLog log = new PersistentRaftLog(wal);
            for (int i = 1; i <= 10; i++) log.append(entry(1, i));
            log.resetToSnapshot(LogIndex.of(10), Term.of(1));

            assertThat(log.size()).isZero();
            assertThat(log.snapshotIndex().value()).isEqualTo(10);
            assertThat(log.lastEntry().index().value()).isEqualTo(10);
            assertThat(log.lastEntry().term().value()).isEqualTo(1);
        }
    }

    @Test
    void appendAfterSnapshotIndexedCorrectly() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            PersistentRaftLog log = new PersistentRaftLog(wal);
            for (int i = 1; i <= 5; i++) log.append(entry(1, i));
            log.resetToSnapshot(LogIndex.of(5), Term.of(1));

            log.append(entry(2, 6));
            log.append(entry(2, 7));

            assertThat(log.size()).isEqualTo(2);
            assertThat(log.entryAt(LogIndex.of(6)).term().value()).isEqualTo(2);
            assertThat(log.entriesFrom(LogIndex.of(6))).hasSize(2);
        }
    }

    @Test
    void recoverySkipsEntriesBelowSnapshotBase() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        // Write entries 1-10 to WAL
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            PersistentRaftLog log = new PersistentRaftLog(wal);
            for (int i = 1; i <= 10; i++) log.append(entry(1, i));
        }
        // Reopen with snapshotBase at 7 — entries 1-7 should be skipped
        LogEntry snapBase = new LogEntry(Term.of(1), LogIndex.of(7), new byte[0]);
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            PersistentRaftLog log2 = new PersistentRaftLog(wal2, snapBase);
            // Only entries 8, 9, 10 should be loaded
            assertThat(log2.size()).isEqualTo(3);
            assertThat(log2.entryAt(LogIndex.of(8)).index().value()).isEqualTo(8);
            assertThat(log2.snapshotIndex().value()).isEqualTo(7);
        }
    }
}
