package io.kronos.server.kv;

import io.kronos.common.model.LogIndex;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class CommandEncoderTest {

    private final KvStateMachine sm = new KvStateMachine();
    private static final LogIndex IDX = LogIndex.ONE;

    @Test
    void putRoundTrip() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        sm.apply(IDX, CommandEncoder.encodePut("foo", value));

        assertThat(sm.get("foo")).isEqualTo(value);
    }

    @Test
    void deleteRoundTrip() {
        sm.apply(IDX, CommandEncoder.encodePut("bar", new byte[]{1, 2, 3}));
        sm.apply(LogIndex.of(2), CommandEncoder.encodeDelete("bar"));

        assertThat(sm.get("bar")).isNull();
    }

    @Test
    void casRoundTrip_winCase() {
        byte[] initial  = "v1".getBytes(StandardCharsets.UTF_8);
        byte[] newValue = "v2".getBytes(StandardCharsets.UTF_8);

        sm.apply(IDX, CommandEncoder.encodePut("k", initial));
        sm.apply(LogIndex.of(2), CommandEncoder.encodeCas("k", initial, newValue));

        assertThat(sm.get("k")).isEqualTo(newValue);
    }

    @Test
    void casRoundTrip_loseCase() {
        byte[] initial  = "v1".getBytes(StandardCharsets.UTF_8);
        byte[] wrong    = "vX".getBytes(StandardCharsets.UTF_8);
        byte[] newValue = "v2".getBytes(StandardCharsets.UTF_8);

        sm.apply(IDX, CommandEncoder.encodePut("k", initial));
        sm.apply(LogIndex.of(2), CommandEncoder.encodeCas("k", wrong, newValue));

        assertThat(sm.get("k")).isEqualTo(initial);
    }

    @Test
    void putEmptyValue() {
        sm.apply(IDX, CommandEncoder.encodePut("empty", new byte[0]));

        assertThat(sm.get("empty")).isEqualTo(new byte[0]);
    }

    @Test
    void deleteNonExistentKey_noError() {
        sm.apply(IDX, CommandEncoder.encodeDelete("ghost"));
        assertThat(sm.get("ghost")).isNull();
    }
}
