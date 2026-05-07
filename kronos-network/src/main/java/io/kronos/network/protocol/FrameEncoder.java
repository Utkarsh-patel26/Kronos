package io.kronos.network.protocol;

import java.nio.ByteBuffer;

public final class FrameEncoder {

    public static final int   HEADER_SIZE = 12;
    public static final short MAGIC       = 0x4B52;  // "KR"
    public static final int   MAX_BODY_LEN = Frame.MAX_BODY_LEN;

    private FrameEncoder() {}

    /**
     * Encodes a frame to a flipped, ready-to-read ByteBuffer.
     *
     * Layout (big-endian):
     *   [2] magic  = 0x4B52
     *   [2] type code
     *   [4] reqId
     *   [4] body length
     *   [N] body bytes
     */
    public static ByteBuffer encode(Frame frame) {
        byte[] body = frame.bodyRaw();
        ByteBuffer buf = ByteBuffer.allocate(HEADER_SIZE + body.length);
        buf.putShort(MAGIC);
        buf.putShort(frame.type().code);
        buf.putInt(frame.reqId());
        buf.putInt(body.length);
        buf.put(body);
        buf.flip();
        return buf;
    }
}
