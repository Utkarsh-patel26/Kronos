package io.kronos.common.config;

import io.kronos.common.model.NodeId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PeerConfigTest {

    @Test
    void happyPath() {
        PeerConfig p = new PeerConfig(NodeId.of("n2"), "localhost", 9002);
        assertThat(p.toString()).isEqualTo("n2@localhost:9002");
    }

    @Test
    void rejectsBlankHost() {
        assertThatThrownBy(() -> new PeerConfig(NodeId.of("n2"), "", 9002))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsOutOfRangePort() {
        assertThatThrownBy(() -> new PeerConfig(NodeId.of("n2"), "host", 0))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new PeerConfig(NodeId.of("n2"), "host", 70_000))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
