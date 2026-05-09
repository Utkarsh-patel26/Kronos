package io.kronos.raft.log;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class InMemoryRaftLogTest {

    private InMemoryRaftLog log;

    @BeforeEach
    void setUp() {
        log = new InMemoryRaftLog();
    }

    @Test
    void emptyLogReturnsSentinel() {
        assertThat(log.lastEntry().index()).isEqualTo(LogIndex.ZERO);
        assertThat(log.lastEntry().term()).isEqualTo(Term.ZERO);
    }

    @Test
    void appendUpdatesLastEntry() {
        LogEntry e = entry(1, 1);
        log.append(e);
        assertThat(log.lastEntry()).isEqualTo(e);
    }

    @Test
    void sizeReflectsAppends() {
        assertThat(log.size()).isZero();
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        assertThat(log.size()).isEqualTo(2);
    }

    @Test
    void entryAtReturnsCorrectEntry() {
        LogEntry first  = entry(1, 1);
        LogEntry second = entry(2, 2);
        log.append(first);
        log.append(second);

        assertThat(log.entryAt(LogIndex.of(1))).isEqualTo(first);
        assertThat(log.entryAt(LogIndex.of(2))).isEqualTo(second);
    }

    @Test
    void entryAtZeroReturnsSentinel() {
        assertThat(log.entryAt(LogIndex.ZERO).index()).isEqualTo(LogIndex.ZERO);
    }

    @Test
    void entriesFromReturnsSlice() {
        LogEntry e1 = entry(1, 1);
        LogEntry e2 = entry(1, 2);
        LogEntry e3 = entry(1, 3);
        log.append(e1);
        log.append(e2);
        log.append(e3);

        List<LogEntry> from2 = log.entriesFrom(LogIndex.of(2));
        assertThat(from2).containsExactly(e2, e3);
    }

    @Test
    void entriesFromZeroReturnsAll() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));

        assertThat(log.entriesFrom(LogIndex.ZERO)).hasSize(2);
    }

    @Test
    void entriesFromBeyondEndReturnsEmpty() {
        log.append(entry(1, 1));
        assertThat(log.entriesFrom(LogIndex.of(5))).isEmpty();
    }

    @Test
    void truncateFromRemovesTrailingEntries() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));
        log.append(entry(1, 3));

        log.truncateFrom(LogIndex.of(2));

        assertThat(log.size()).isEqualTo(1);
        assertThat(log.lastEntry().index()).isEqualTo(LogIndex.of(1));
    }

    @Test
    void truncateFromZeroClearsAll() {
        log.append(entry(1, 1));
        log.append(entry(1, 2));

        log.truncateFrom(LogIndex.ZERO);

        assertThat(log.size()).isZero();
        assertThat(log.lastEntry().index()).isEqualTo(LogIndex.ZERO);
    }

    // -----------------------------------------------------------------------

    private static LogEntry entry(long term, long index) {
        return new LogEntry(Term.of(term), LogIndex.of(index), new byte[0]);
    }
}
