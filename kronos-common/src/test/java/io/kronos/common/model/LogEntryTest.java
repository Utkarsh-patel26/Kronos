package io.kronos.common.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LogEntryTest {

    @Test
    void payloadIsDefensivelyCopiedOnConstruction() {
        byte[] original = {1, 2, 3};
        LogEntry entry = new LogEntry(Term.of(1), LogIndex.of(1), original);
        original[0] = 99;
        assertThat(entry.payload()[0]).isEqualTo((byte) 1);
    }

    @Test
    void payloadGetterReturnsDefensiveCopy() {
        LogEntry entry = new LogEntry(Term.of(1), LogIndex.of(1), new byte[]{7, 8, 9});
        byte[] copy = entry.payload();
        copy[0] = 99;
        assertThat(entry.payload()[0]).isEqualTo((byte) 7);
    }

    @Test
    void equalsByContents() {
        LogEntry a = new LogEntry(Term.of(2), LogIndex.of(5), new byte[]{1, 2, 3});
        LogEntry b = new LogEntry(Term.of(2), LogIndex.of(5), new byte[]{1, 2, 3});
        LogEntry c = new LogEntry(Term.of(2), LogIndex.of(5), new byte[]{1, 2, 4});
        LogEntry d = new LogEntry(Term.of(3), LogIndex.of(5), new byte[]{1, 2, 3});

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(c);
        assertThat(a).isNotEqualTo(d);
    }

    @Test
    void payloadLengthReflectsBytes() {
        LogEntry e = new LogEntry(Term.of(1), LogIndex.of(1), new byte[]{1, 2, 3, 4});
        assertThat(e.payloadLength()).isEqualTo(4);
    }

    @Test
    void rejectsNulls() {
        assertThatThrownBy(() -> new LogEntry(null, LogIndex.of(1), new byte[0]))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new LogEntry(Term.of(1), null, new byte[0]))
            .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new LogEntry(Term.of(1), LogIndex.of(1), null))
            .isInstanceOf(NullPointerException.class);
    }
}
