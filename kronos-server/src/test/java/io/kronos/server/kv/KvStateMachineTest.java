package io.kronos.server.kv;

import io.kronos.common.model.LogIndex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class KvStateMachineTest {

    private KvStateMachine sm;

    @BeforeEach
    void setUp() { sm = new KvStateMachine(); }

    @Test
    void putStoresValue() {
        apply(1, CommandEncoder.encodePut("x", bytes("hello")));
        assertThat(sm.get("x")).isEqualTo(bytes("hello"));
    }

    @Test
    void putOverwritesExisting() {
        apply(1, CommandEncoder.encodePut("x", bytes("a")));
        apply(2, CommandEncoder.encodePut("x", bytes("b")));
        assertThat(sm.get("x")).isEqualTo(bytes("b"));
    }

    @Test
    void deleteRemovesKey() {
        apply(1, CommandEncoder.encodePut("x", bytes("v")));
        apply(2, CommandEncoder.encodeDelete("x"));
        assertThat(sm.get("x")).isNull();
        assertThat(sm.size()).isEqualTo(0);
    }

    @Test
    void casSwapsWhenMatches() {
        apply(1, CommandEncoder.encodePut("k", bytes("old")));
        apply(2, CommandEncoder.encodeCas("k", bytes("old"), bytes("new")));
        assertThat(sm.get("k")).isEqualTo(bytes("new"));
    }

    @Test
    void casIgnoresWhenMismatch() {
        apply(1, CommandEncoder.encodePut("k", bytes("old")));
        apply(2, CommandEncoder.encodeCas("k", bytes("wrong"), bytes("new")));
        assertThat(sm.get("k")).isEqualTo(bytes("old"));
    }

    @Test
    void casOnMissingKeyWithNullExpected() {
        // expected = empty byte array ≠ null (key not present); should not insert
        apply(1, CommandEncoder.encodeCas("missing", new byte[0], bytes("v")));
        assertThat(sm.get("missing")).isNull();
    }

    @Test
    void unknownCommandTypeIsIgnored() {
        apply(1, new byte[]{(byte) 0xFF});
        assertThat(sm.size()).isEqualTo(0);
    }

    @Test
    void emptyCommandIsIgnored() {
        apply(1, new byte[0]);
        assertThat(sm.size()).isEqualTo(0);
    }

    // -----------------------------------------------------------------------
    // Snapshot round-trip
    // -----------------------------------------------------------------------

    @Test
    void snapshotRoundTrip() {
        apply(1, CommandEncoder.encodePut("a", bytes("1")));
        apply(2, CommandEncoder.encodePut("b", bytes("2")));
        apply(3, CommandEncoder.encodePut("c", bytes("3")));

        byte[] snapshot = sm.takeSnapshot();

        KvStateMachine restored = new KvStateMachine();
        restored.installSnapshot(snapshot);

        assertThat(restored.get("a")).isEqualTo(bytes("1"));
        assertThat(restored.get("b")).isEqualTo(bytes("2"));
        assertThat(restored.get("c")).isEqualTo(bytes("3"));
        assertThat(restored.size()).isEqualTo(3);
    }

    @Test
    void installSnapshotReplacesExistingState() {
        apply(1, CommandEncoder.encodePut("old", bytes("v")));
        byte[] snapshot = sm.takeSnapshot();

        KvStateMachine sm2 = new KvStateMachine();
        sm2.apply(LogIndex.ONE, CommandEncoder.encodePut("other", bytes("x")));
        sm2.installSnapshot(snapshot);

        assertThat(sm2.get("old")).isEqualTo(bytes("v"));
        assertThat(sm2.get("other")).isNull();
    }

    @Test
    void installEmptySnapshot() {
        apply(1, CommandEncoder.encodePut("x", bytes("v")));
        sm.installSnapshot(new byte[0]);
        assertThat(sm.size()).isEqualTo(0);
    }

    @Test
    void emptySnapshotRoundTrip() {
        byte[] snapshot = sm.takeSnapshot();
        KvStateMachine restored = new KvStateMachine();
        restored.installSnapshot(snapshot);
        assertThat(restored.size()).isEqualTo(0);
    }

    // -----------------------------------------------------------------------

    private void apply(long idx, byte[] command) {
        sm.apply(LogIndex.of(idx), command);
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
