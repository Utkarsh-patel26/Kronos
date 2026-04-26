package io.kronos.common.model;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NodeIdTest {

    @Test
    void equalsByValue() {
        assertThat(NodeId.of("n1")).isEqualTo(NodeId.of("n1"));
        assertThat(NodeId.of("n1")).isNotEqualTo(NodeId.of("n2"));
    }

    @Test
    void toStringIsValue() {
        assertThat(NodeId.of("leader-a").toString()).isEqualTo("leader-a");
    }

    @Test
    void rejectsNull() {
        assertThatThrownBy(() -> new NodeId(null))
            .isInstanceOf(NullPointerException.class);
    }

    @Test
    void rejectsBlank() {
        assertThatThrownBy(() -> new NodeId(""))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new NodeId("   "))
            .isInstanceOf(IllegalArgumentException.class);
    }
}
