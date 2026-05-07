package io.kronos.network.protocol;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public final class FrameDecoder {

    private FrameDecoder() {}

    /**
     * Decodes zero or more complete frames from {@code buf}.
     *
     * <p>The buffer must be flipped (position=0, limit=bytes available) before
     * calling. After returning, any unconsumed bytes remain at the start so the
     * caller can call {@link ByteBuffer#compact()} and continue accumulating.
     *
     * <p>Handles partial reads: if the buffer holds fewer bytes than the next
     * frame requires, the buffer is reset to just before that frame's header
     * and decoding stops.
     */
    public static List<Frame> decode(ByteBuffer buf) {
        List<Frame> out = new ArrayList<>();
        while (buf.remaining() >= FrameEncoder.HEADER_SIZE) {
            buf.mark();

            short magic = buf.getShort();
            if (magic != FrameEncoder.MAGIC) {
                throw new ProtocolException(
                    "bad magic: 0x" + Integer.toHexString(magic & 0xFFFF));
            }

            short typeCode = buf.getShort();
            int   reqId    = buf.getInt();
            int   bodyLen  = buf.getInt();

            if (bodyLen < 0 || bodyLen > FrameEncoder.MAX_BODY_LEN) {
                throw new ProtocolException("invalid body length: " + bodyLen);
            }
            if (buf.remaining() < bodyLen) {
                buf.reset(); // wait for more bytes
                break;
            }

            byte[] body = new byte[bodyLen];
            buf.get(body);
            out.add(new Frame(MessageType.fromCode(typeCode), reqId, body));
        }
        return out;
    }
}
