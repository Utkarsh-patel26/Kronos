package io.kronos.network.protocol;

public enum MessageType {

    APPEND_ENTRIES_REQUEST   ((short) 0x01),
    APPEND_ENTRIES_RESPONSE  ((short) 0x02),
    REQUEST_VOTE_REQUEST     ((short) 0x03),
    REQUEST_VOTE_RESPONSE    ((short) 0x04),
    INSTALL_SNAPSHOT_REQUEST ((short) 0x05),
    INSTALL_SNAPSHOT_RESPONSE((short) 0x06);

    public final short code;

    MessageType(short code) {
        this.code = code;
    }

    public static MessageType fromCode(short code) {
        for (MessageType t : values()) {
            if (t.code == code) return t;
        }
        throw new ProtocolException("unknown message type: 0x" + Integer.toHexString(code & 0xFFFF));
    }

    public MessageType responseType() {
        return switch (this) {
            case APPEND_ENTRIES_REQUEST    -> APPEND_ENTRIES_RESPONSE;
            case REQUEST_VOTE_REQUEST      -> REQUEST_VOTE_RESPONSE;
            case INSTALL_SNAPSHOT_REQUEST  -> INSTALL_SNAPSHOT_RESPONSE;
            default -> throw new ProtocolException("no response type for: " + this);
        };
    }

    public boolean isRequest() {
        return this == APPEND_ENTRIES_REQUEST
            || this == REQUEST_VOTE_REQUEST
            || this == INSTALL_SNAPSHOT_REQUEST;
    }
}
