package io.kronos.network.protocol;

import java.util.Arrays;
import java.util.Objects;

public final class Frame {

    /** Maximum permitted body size (64 MiB). */
    public static final int MAX_BODY_LEN = 64 * 1024 * 1024;

    private final MessageType type;
    private final int         reqId;
    private final byte[]      body;

    public Frame(MessageType type, int reqId, byte[] body) {
        this.type  = Objects.requireNonNull(type, "type");
        this.reqId = reqId;
        Objects.requireNonNull(body, "body");
        if (body.length > MAX_BODY_LEN) {
            throw new ProtocolException("body too large: " + body.length);
        }
        this.body = body.clone();
    }

    public MessageType type()  { return type; }
    public int         reqId() { return reqId; }

    /** Defensive copy on every call — callers cannot mutate frame state. */
    public byte[] body() { return body.clone(); }

    /** Raw body reference for internal use by codec and encoder — no copy. */
    byte[] bodyRaw() { return body; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Frame f)) return false;
        return reqId == f.reqId && type == f.type && Arrays.equals(body, f.body);
    }

    @Override
    public int hashCode() {
        int h = Objects.hash(type, reqId);
        return 31 * h + Arrays.hashCode(body);
    }

    @Override
    public String toString() {
        return "Frame{type=" + type + ", reqId=" + reqId + ", bodyLen=" + body.length + "}";
    }
}
