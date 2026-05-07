package io.kronos.network.protocol;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.assertj.core.api.Assertions.*;

class FrameEncoderTest {

    @Test
    void encodedBufferHasCorrectMagic() {
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, new byte[0]);
        ByteBuffer buf = FrameEncoder.encode(frame);

        short magic = buf.getShort(0);
        assertThat(magic).isEqualTo(FrameEncoder.MAGIC);
    }

    @Test
    void encodedBufferHasCorrectTypeCode() {
        Frame frame = new Frame(MessageType.APPEND_ENTRIES_RESPONSE, 7, new byte[0]);
        ByteBuffer buf = FrameEncoder.encode(frame);

        short code = buf.getShort(2);
        assertThat(code).isEqualTo(MessageType.APPEND_ENTRIES_RESPONSE.code);
    }

    @Test
    void encodedBufferHasCorrectReqId() {
        int reqId = 0x1A2B3C4D;
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, reqId, new byte[0]);
        ByteBuffer buf = FrameEncoder.encode(frame);

        int encodedReqId = buf.getInt(4);
        assertThat(encodedReqId).isEqualTo(reqId);
    }

    @Test
    void encodedBufferHasCorrectBodyLength() {
        byte[] body = {1, 2, 3, 4, 5};
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, body);
        ByteBuffer buf = FrameEncoder.encode(frame);

        int bodyLen = buf.getInt(8);
        assertThat(bodyLen).isEqualTo(5);
    }

    @Test
    void encodedBufferBodyMatchesInput() {
        byte[] body = {10, 20, 30};
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, body);
        ByteBuffer buf = FrameEncoder.encode(frame);

        buf.position(FrameEncoder.HEADER_SIZE);
        byte[] encodedBody = new byte[3];
        buf.get(encodedBody);
        assertThat(encodedBody).isEqualTo(body);
    }

    @Test
    void totalSizeIsHeaderPlusBodyLength() {
        byte[] body = new byte[100];
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, body);
        ByteBuffer buf = FrameEncoder.encode(frame);

        assertThat(buf.remaining()).isEqualTo(FrameEncoder.HEADER_SIZE + 100);
    }

    @Test
    void emptyBodyEncodesCorrectly() {
        Frame frame = new Frame(MessageType.REQUEST_VOTE_RESPONSE, 99, new byte[0]);
        ByteBuffer buf = FrameEncoder.encode(frame);

        assertThat(buf.remaining()).isEqualTo(FrameEncoder.HEADER_SIZE);
        assertThat(buf.getInt(8)).isEqualTo(0); // bodyLen = 0
    }

    @Test
    void bufferIsReadyToRead() {
        Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, new byte[4]);
        ByteBuffer buf = FrameEncoder.encode(frame);

        // position=0, limit=totalSize — ready to read without flip()
        assertThat(buf.position()).isEqualTo(0);
        assertThat(buf.limit()).isEqualTo(FrameEncoder.HEADER_SIZE + 4);
    }
}
