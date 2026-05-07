package io.kronos.network.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.*;

class FrameDecoderTest {

    @Test
    void decodesCompleteFrame() {
        Frame original = new Frame(MessageType.REQUEST_VOTE_REQUEST, 42, new byte[]{1, 2, 3});
        ByteBuffer buf = FrameEncoder.encode(original);

        List<Frame> frames = FrameDecoder.decode(buf);

        assertThat(frames).hasSize(1);
        assertThat(frames.get(0)).isEqualTo(original);
    }

    @Test
    void decodesMultipleFramesFromSingleBuffer() {
        Frame f1 = new Frame(MessageType.REQUEST_VOTE_REQUEST,    1, new byte[]{1});
        Frame f2 = new Frame(MessageType.APPEND_ENTRIES_REQUEST,  2, new byte[]{2, 3});
        Frame f3 = new Frame(MessageType.INSTALL_SNAPSHOT_REQUEST, 3, new byte[0]);

        ByteBuffer b1 = FrameEncoder.encode(f1);
        ByteBuffer b2 = FrameEncoder.encode(f2);
        ByteBuffer b3 = FrameEncoder.encode(f3);

        ByteBuffer combined = ByteBuffer.allocate(b1.remaining() + b2.remaining() + b3.remaining());
        combined.put(b1).put(b2).put(b3);
        combined.flip();

        List<Frame> frames = FrameDecoder.decode(combined);

        assertThat(frames).containsExactly(f1, f2, f3);
    }

    @Test
    void returnsEmptyListWhenBufferIsEmpty() {
        ByteBuffer buf = ByteBuffer.allocate(0);
        assertThat(FrameDecoder.decode(buf)).isEmpty();
    }

    @Test
    void returnsEmptyListWhenBufferHasFewerThanHeaderBytes() {
        ByteBuffer buf = ByteBuffer.wrap(new byte[]{0x4B, 0x52, 0x00}); // only 3 bytes
        assertThat(FrameDecoder.decode(buf)).isEmpty();
        assertThat(buf.position()).isEqualTo(0); // nothing consumed
    }

    @Test
    void handlesPartialBodyGracefully() {
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, new byte[]{10, 20, 30});
        ByteBuffer full = FrameEncoder.encode(frame);

        // Truncate to header + 1 body byte (missing 2)
        byte[] partial = new byte[FrameEncoder.HEADER_SIZE + 1];
        full.get(partial);

        ByteBuffer buf = ByteBuffer.wrap(partial);
        List<Frame> frames = FrameDecoder.decode(buf);

        assertThat(frames).isEmpty();
        assertThat(buf.position()).isEqualTo(0); // reset to mark, nothing consumed
    }

    @Test
    void handlesPartialHeaderGracefully() {
        // Only 6 bytes — not even a full header
        ByteBuffer buf = ByteBuffer.wrap(new byte[]{0x4B, 0x52, 0x00, 0x01, 0x00, 0x00});
        assertThat(FrameDecoder.decode(buf)).isEmpty();
        assertThat(buf.position()).isEqualTo(0);
    }

    @Test
    void throwsOnBadMagic() {
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, new byte[]{1});
        ByteBuffer buf = FrameEncoder.encode(frame);

        // Corrupt the magic bytes
        buf.putShort(0, (short) 0xDEAD);

        assertThatThrownBy(() -> FrameDecoder.decode(buf))
            .isInstanceOf(ProtocolException.class)
            .hasMessageContaining("bad magic");
    }

    @Test
    void feedOneByteAtATimeProducesCompleteFrame() {
        Frame original = new Frame(MessageType.APPEND_ENTRIES_REQUEST, 7, new byte[]{11, 22, 33, 44});
        ByteBuffer encoded = FrameEncoder.encode(original);
        byte[] bytes = new byte[encoded.remaining()];
        encoded.get(bytes);

        ByteBuffer accumulator = ByteBuffer.allocate(1024);
        List<Frame> frames = new ArrayList<>();

        for (byte b : bytes) {
            accumulator.put(b);
            accumulator.flip();
            frames.addAll(FrameDecoder.decode(accumulator));
            accumulator.compact();
        }

        assertThat(frames).hasSize(1);
        assertThat(frames.get(0)).isEqualTo(original);
    }

    @Test
    void feedTwoFramesOneByteAtATimeProducesBothFrames() {
        Frame f1 = new Frame(MessageType.REQUEST_VOTE_REQUEST,   1, new byte[]{1, 2});
        Frame f2 = new Frame(MessageType.REQUEST_VOTE_RESPONSE,  2, new byte[]{3});

        ByteBuffer e1 = FrameEncoder.encode(f1);
        ByteBuffer e2 = FrameEncoder.encode(f2);
        byte[] bytes = new byte[e1.remaining() + e2.remaining()];
        ByteBuffer.wrap(bytes).put(e1).put(e2);

        ByteBuffer acc = ByteBuffer.allocate(1024);
        List<Frame> frames = new ArrayList<>();

        for (byte b : bytes) {
            acc.put(b);
            acc.flip();
            frames.addAll(FrameDecoder.decode(acc));
            acc.compact();
        }

        assertThat(frames).containsExactly(f1, f2);
    }

    @Test
    void roundTripAllMessageTypes() {
        for (MessageType type : MessageType.values()) {
            byte[] body = {(byte) type.code, 0x01};
            Frame original = new Frame(type, type.code, body);
            ByteBuffer buf = FrameEncoder.encode(original);
            List<Frame> decoded = FrameDecoder.decode(buf);
            assertThat(decoded).hasSize(1);
            assertThat(decoded.get(0)).isEqualTo(original);
        }
    }
}
