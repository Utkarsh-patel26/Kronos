package io.kronos.network.protocol;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class MessageTypeTest {

    @Test
    void fromCodeResolvesAllSixTypes() {
        assertThat(MessageType.fromCode((short) 0x01)).isEqualTo(MessageType.APPEND_ENTRIES_REQUEST);
        assertThat(MessageType.fromCode((short) 0x02)).isEqualTo(MessageType.APPEND_ENTRIES_RESPONSE);
        assertThat(MessageType.fromCode((short) 0x03)).isEqualTo(MessageType.REQUEST_VOTE_REQUEST);
        assertThat(MessageType.fromCode((short) 0x04)).isEqualTo(MessageType.REQUEST_VOTE_RESPONSE);
        assertThat(MessageType.fromCode((short) 0x05)).isEqualTo(MessageType.INSTALL_SNAPSHOT_REQUEST);
        assertThat(MessageType.fromCode((short) 0x06)).isEqualTo(MessageType.INSTALL_SNAPSHOT_RESPONSE);
    }

    @Test
    void fromCodeThrowsOnUnknownCode() {
        assertThatThrownBy(() -> MessageType.fromCode((short) 0xFF))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining("unknown message type");
    }

    @Test
    void responseTypeReturnsCorrectPair() {
        assertThat(MessageType.APPEND_ENTRIES_REQUEST.responseType())
            .isEqualTo(MessageType.APPEND_ENTRIES_RESPONSE);
        assertThat(MessageType.REQUEST_VOTE_REQUEST.responseType())
            .isEqualTo(MessageType.REQUEST_VOTE_RESPONSE);
        assertThat(MessageType.INSTALL_SNAPSHOT_REQUEST.responseType())
            .isEqualTo(MessageType.INSTALL_SNAPSHOT_RESPONSE);
    }

    @Test
    void responseTypeThrowsForResponseTypes() {
        assertThatThrownBy(() -> MessageType.APPEND_ENTRIES_RESPONSE.responseType())
            .isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> MessageType.REQUEST_VOTE_RESPONSE.responseType())
            .isInstanceOf(ProtocolException.class);
        assertThatThrownBy(() -> MessageType.INSTALL_SNAPSHOT_RESPONSE.responseType())
            .isInstanceOf(ProtocolException.class);
    }

    @Test
    void isRequestReturnsTrueOnlyForRequests() {
        assertThat(MessageType.APPEND_ENTRIES_REQUEST.isRequest()).isTrue();
        assertThat(MessageType.REQUEST_VOTE_REQUEST.isRequest()).isTrue();
        assertThat(MessageType.INSTALL_SNAPSHOT_REQUEST.isRequest()).isTrue();

        assertThat(MessageType.APPEND_ENTRIES_RESPONSE.isRequest()).isFalse();
        assertThat(MessageType.REQUEST_VOTE_RESPONSE.isRequest()).isFalse();
        assertThat(MessageType.INSTALL_SNAPSHOT_RESPONSE.isRequest()).isFalse();
    }

    @Test
    void codeRoundTripsForAllTypes() {
        for (MessageType t : MessageType.values()) {
            assertThat(MessageType.fromCode(t.code)).isEqualTo(t);
        }
    }
}
