package io.kronos.storage.snapshot;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.*;

class SnapshotManagerTest {

    @TempDir
    Path tmpDir;

    private SnapshotManager mgr(String sub) throws IOException {
        return new SnapshotManager(tmpDir.resolve(sub));
    }

    // -----------------------------------------------------------------------
    // Basic save / load
    // -----------------------------------------------------------------------

    @Test
    void saveAndLoadRoundTrip() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        byte[] state = "hello-snapshot".getBytes();

        SnapshotMetadata meta = mgr.save(LogIndex.of(42), Term.of(3), state);

        assertThat(meta.lastIncludedIndex().value()).isEqualTo(42);
        assertThat(meta.lastIncludedTerm().value()).isEqualTo(3);
        assertThat(mgr.loadState(meta)).isEqualTo(state);
    }

    @Test
    void latestSnapshotReturnsNullWhenNoneExist() throws IOException {
        assertThat(mgr("snaps").latestSnapshot()).isNull();
    }

    @Test
    void latestSnapshotReturnsNewest() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        mgr.save(LogIndex.of(10), Term.of(1), "state-10".getBytes());
        mgr.save(LogIndex.of(20), Term.of(2), "state-20".getBytes());

        SnapshotMetadata latest = mgr.latestSnapshot();
        assertThat(latest).isNotNull();
        assertThat(latest.lastIncludedIndex().value()).isEqualTo(20);
    }

    @Test
    void oldSnapshotsAreDeletedAfterSave() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        mgr.save(LogIndex.of(10), Term.of(1), "old".getBytes());
        mgr.save(LogIndex.of(20), Term.of(2), "new".getBytes());

        long snapCount = Files.list(tmpDir.resolve("snaps"))
            .filter(p -> p.getFileName().toString().endsWith(".snap"))
            .count();
        assertThat(snapCount).isEqualTo(1);
    }

    @Test
    void emptyStateRoundTrips() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        SnapshotMetadata meta = mgr.save(LogIndex.of(1), Term.of(1), new byte[0]);
        assertThat(mgr.loadState(meta)).isEmpty();
    }

    @Test
    void largeStateRoundTrips() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        byte[] big = new byte[512 * 1024]; // 512 KB
        for (int i = 0; i < big.length; i++) big[i] = (byte) (i & 0xFF);
        SnapshotMetadata meta = mgr.save(LogIndex.of(1), Term.of(1), big);
        assertThat(mgr.loadState(meta)).isEqualTo(big);
    }

    // -----------------------------------------------------------------------
    // Corruption detection
    // -----------------------------------------------------------------------

    @Test
    void corruptedCrcThrows() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        SnapshotMetadata meta = mgr.save(LogIndex.of(5), Term.of(1), "data".getBytes());

        byte[] raw = Files.readAllBytes(meta.path());
        raw[raw.length - 2] ^= (byte) 0xFF; // flip a bit in CRC
        Files.write(meta.path(), raw);

        assertThatThrownBy(() -> mgr.loadState(meta))
            .isInstanceOf(CorruptedSnapshotException.class)
            .hasMessageContaining("CRC32");
    }

    @Test
    void corruptedMagicThrows() throws IOException {
        SnapshotManager mgr = mgr("snaps");
        SnapshotMetadata meta = mgr.save(LogIndex.of(5), Term.of(1), "data".getBytes());

        byte[] raw = Files.readAllBytes(meta.path());
        raw[0] ^= (byte) 0xFF; // corrupt magic byte
        Files.write(meta.path(), raw);

        assertThatThrownBy(() -> mgr.loadState(meta))
            .isInstanceOf(CorruptedSnapshotException.class)
            .hasMessageContaining("magic");
    }

    // -----------------------------------------------------------------------
    // Chunked transfer (InstallSnapshot RPC)
    // -----------------------------------------------------------------------

    @Test
    void chunkAssemblyRoundTrips() throws IOException {
        // Simulate a leader writing a snapshot and a follower receiving it in chunks.
        SnapshotManager leader = mgr("leader");
        byte[] state = "chunked-state-data".getBytes();
        SnapshotMetadata leaderMeta = leader.save(LogIndex.of(7), Term.of(2), state);
        byte[] raw = leader.readRaw(leaderMeta);

        // Split into two chunks and assemble on the follower side
        SnapshotManager follower = mgr("follower");
        int half = raw.length / 2;
        byte[] chunk1 = new byte[half];
        byte[] chunk2 = new byte[raw.length - half];
        System.arraycopy(raw, 0, chunk1, 0, half);
        System.arraycopy(raw, half, chunk2, 0, chunk2.length);

        follower.writeChunk(0, chunk1);
        follower.writeChunk(half, chunk2);
        SnapshotMetadata assembled = follower.finalizeChunked(LogIndex.of(7), Term.of(2));

        assertThat(follower.loadState(assembled)).isEqualTo(state);
        assertThat(assembled.lastIncludedIndex().value()).isEqualTo(7);
        assertThat(assembled.lastIncludedTerm().value()).isEqualTo(2);
    }

    @Test
    void finalizeChunkedWithoutWriteChunkThrows() throws IOException {
        assertThatThrownBy(() -> mgr("snaps").finalizeChunked(LogIndex.of(1), Term.of(1)))
            .isInstanceOf(IOException.class);
    }

    @Test
    void newChunkTransferRestartsFromOffset0() throws IOException {
        // Simulate two separate snapshot transfers arriving at a follower;
        // the second one (offset=0) must overwrite the first incomplete transfer.
        SnapshotManager leader1 = mgr("leader1");
        SnapshotManager leader2 = mgr("leader2");
        byte[] state1 = "state-A".getBytes();
        byte[] state2 = "state-B-longer".getBytes();
        byte[] raw1 = leader1.readRaw(leader1.save(LogIndex.of(1), Term.of(1), state1));
        byte[] raw2 = leader2.readRaw(leader2.save(LogIndex.of(2), Term.of(1), state2));

        SnapshotManager follower = mgr("follower");
        // First incomplete transfer (offset=0 starts temp file)
        follower.writeChunk(0, raw1);
        // Second transfer starts fresh — offset=0 must reset the temp file
        follower.writeChunk(0, raw2);
        SnapshotMetadata result = follower.finalizeChunked(LogIndex.of(2), Term.of(1));

        assertThat(follower.loadState(result)).isEqualTo(state2);
    }
}
