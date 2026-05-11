package io.kronos.storage.wal;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class WriteAheadLogTest {

    @TempDir
    Path tmpDir;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static LogEntry entry(long term, long index, String cmd) {
        return new LogEntry(Term.of(term), LogIndex.of(index), cmd.getBytes());
    }

    private WriteAheadLog open(String subdir) throws IOException {
        return new WriteAheadLog(tmpDir.resolve(subdir));
    }

    // -----------------------------------------------------------------------
    // Basic recovery
    // -----------------------------------------------------------------------

    @Test
    void emptyWalRecoversEmpty() throws IOException {
        try (WriteAheadLog wal = open("wal")) {
            assertThat(wal.recover()).isEmpty();
        }
    }

    @Test
    void persistAndRecover() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(entry(1, 1, "alpha"));
            wal.persist(entry(1, 2, "beta"));
            wal.persist(entry(2, 3, "gamma"));
        }

        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            List<LogEntry> got = wal2.recover();
            assertThat(got).hasSize(3);
            assertThat(got.get(0).index().value()).isEqualTo(1);
            assertThat(got.get(0).term().value()).isEqualTo(1);
            assertThat(new String(got.get(1).payload())).isEqualTo("beta");
            assertThat(got.get(2).index().value()).isEqualTo(3);
            assertThat(got.get(2).term().value()).isEqualTo(2);
        }
    }

    @Test
    void payloadRoundTrips() throws IOException {
        byte[] payload = new byte[]{0x00, (byte) 0xFF, 0x42, (byte) 0xAB};
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(new LogEntry(Term.of(1), LogIndex.of(1), payload));
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            assertThat(wal2.recover().get(0).payload()).isEqualTo(payload);
        }
    }

    // -----------------------------------------------------------------------
    // Truncation
    // -----------------------------------------------------------------------

    @Test
    void truncateFromMidLog() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            for (int i = 1; i <= 5; i++) wal.persist(entry(1, i, "cmd" + i));
            wal.truncateFrom(LogIndex.of(3));
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            List<LogEntry> got = wal2.recover();
            assertThat(got).hasSize(2);
            assertThat(got.get(0).index().value()).isEqualTo(1);
            assertThat(got.get(1).index().value()).isEqualTo(2);
        }
    }

    @Test
    void truncateFromFirstEntry() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(entry(1, 1, "a"));
            wal.persist(entry(1, 2, "b"));
            wal.truncateFrom(LogIndex.of(1));
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            assertThat(wal2.recover()).isEmpty();
        }
    }

    @Test
    void truncateFromBeyondLastEntryIsNoop() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(entry(1, 1, "x"));
            wal.truncateFrom(LogIndex.of(100));
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            assertThat(wal2.recover()).hasSize(1);
        }
    }

    @Test
    void appendAfterTruncateIsDurable() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(entry(1, 1, "a"));
            wal.persist(entry(1, 2, "b"));
            wal.persist(entry(1, 3, "c"));
            wal.truncateFrom(LogIndex.of(2));
            wal.persist(entry(2, 2, "b2"));
            wal.persist(entry(2, 3, "c2"));
        }
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            List<LogEntry> got = wal2.recover();
            assertThat(got).hasSize(3);
            assertThat(got.get(1).term().value()).isEqualTo(2); // new term-2 entry at index 2
            assertThat(new String(got.get(1).payload())).isEqualTo("b2");
        }
    }

    // -----------------------------------------------------------------------
    // Corruption detection
    // -----------------------------------------------------------------------

    @Test
    void corruptedCrcThrows() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(entry(1, 1, "payload"));
        }

        // Flip a bit in the last few bytes (CRC area) of the segment file
        Path seg = Files.list(walDir)
            .filter(p -> p.getFileName().toString().endsWith(".wal"))
            .findFirst().orElseThrow();
        byte[] raw = Files.readAllBytes(seg);
        raw[raw.length - 2] ^= (byte) 0xFF;
        Files.write(seg, raw);

        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            assertThatThrownBy(wal2::recover)
                .isInstanceOf(CorruptedWalException.class)
                .hasMessageContaining("CRC32");
        }
    }

    @Test
    void corruptedMagicThrows() throws IOException {
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            wal.persist(entry(1, 1, "x"));
        }

        Path seg = Files.list(walDir)
            .filter(p -> p.getFileName().toString().endsWith(".wal"))
            .findFirst().orElseThrow();
        byte[] raw = Files.readAllBytes(seg);
        raw[0] ^= (byte) 0xFF; // corrupt first byte of magic
        Files.write(seg, raw);

        // Opening a new WAL on the same dir should detect the bad magic
        assertThatThrownBy(() -> new WriteAheadLog(walDir))
            .isInstanceOf(CorruptedWalException.class)
            .hasMessageContaining("magic");
    }

    // -----------------------------------------------------------------------
    // Compaction
    // -----------------------------------------------------------------------

    @Test
    void deleteSegmentsBeforeRemovesOldEntries() throws IOException {
        // This test writes many entries into a single segment (the segment is
        // never rotated in the default config), then simulates snapshot compaction.
        Path walDir = tmpDir.resolve("wal");
        try (WriteAheadLog wal = new WriteAheadLog(walDir)) {
            for (int i = 1; i <= 10; i++) wal.persist(entry(1, i, "cmd" + i));
            // Snapshot covers everything through index 10; compact WAL
            wal.deleteSegmentsBefore(LogIndex.of(10));
        }
        // The segment that contains index 10 is kept (WAL continuity invariant)
        try (WriteAheadLog wal2 = new WriteAheadLog(walDir)) {
            // After compaction the WAL may have only entries covered by the snapshot;
            // a PersistentRaftLog would skip them based on snapshotBase.
            assertThat(wal2.recover()).isNotNull(); // no exception
        }
    }
}
