package io.kronos.common.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ByteUtilsTest {

    @Test
    void shortRoundTrip() {
        byte[] buf = new byte[2];
        ByteUtils.writeShort(buf, 0, (short) 0x1234);
        assertThat(buf).containsExactly(0x12, 0x34);
        assertThat(ByteUtils.readShort(buf, 0)).isEqualTo((short) 0x1234);
    }

    @Test
    void shortHandlesNegative() {
        byte[] buf = new byte[2];
        ByteUtils.writeShort(buf, 0, (short) -1);
        assertThat(ByteUtils.readShort(buf, 0)).isEqualTo((short) -1);
    }

    @Test
    void intRoundTrip() {
        byte[] buf = new byte[4];
        ByteUtils.writeInt(buf, 0, 0x01020304);
        assertThat(buf).containsExactly(0x01, 0x02, 0x03, 0x04);
        assertThat(ByteUtils.readInt(buf, 0)).isEqualTo(0x01020304);
    }

    @Test
    void intHandlesMinAndMax() {
        byte[] buf = new byte[4];
        ByteUtils.writeInt(buf, 0, Integer.MIN_VALUE);
        assertThat(ByteUtils.readInt(buf, 0)).isEqualTo(Integer.MIN_VALUE);
        ByteUtils.writeInt(buf, 0, Integer.MAX_VALUE);
        assertThat(ByteUtils.readInt(buf, 0)).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void longRoundTrip() {
        byte[] buf = new byte[8];
        ByteUtils.writeLong(buf, 0, 0x0102030405060708L);
        assertThat(buf).containsExactly(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08);
        assertThat(ByteUtils.readLong(buf, 0)).isEqualTo(0x0102030405060708L);
    }

    @Test
    void longHandlesMinAndMax() {
        byte[] buf = new byte[8];
        ByteUtils.writeLong(buf, 0, Long.MIN_VALUE);
        assertThat(ByteUtils.readLong(buf, 0)).isEqualTo(Long.MIN_VALUE);
        ByteUtils.writeLong(buf, 0, Long.MAX_VALUE);
        assertThat(ByteUtils.readLong(buf, 0)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void floatRoundTrip() {
        byte[] buf = new byte[4];
        ByteUtils.writeFloat(buf, 0, 3.14159f);
        assertThat(ByteUtils.readFloat(buf, 0)).isEqualTo(3.14159f);
    }

    @Test
    void doubleRoundTrip() {
        byte[] buf = new byte[8];
        ByteUtils.writeDouble(buf, 0, Math.PI);
        assertThat(ByteUtils.readDouble(buf, 0)).isEqualTo(Math.PI);
    }

    @Test
    void byteAndBooleanRoundTrip() {
        byte[] buf = new byte[2];
        ByteUtils.writeByte(buf, 0, (byte) 0x7F);
        ByteUtils.writeBoolean(buf, 1, true);
        assertThat(ByteUtils.readByte(buf, 0)).isEqualTo((byte) 0x7F);
        assertThat(ByteUtils.readBoolean(buf, 1)).isTrue();

        ByteUtils.writeBoolean(buf, 1, false);
        assertThat(ByteUtils.readBoolean(buf, 1)).isFalse();
    }

    @Test
    void writesAtOffset() {
        byte[] buf = new byte[10];
        ByteUtils.writeInt(buf, 3, 0xDEADBEEF);
        assertThat(buf[3] & 0xFF).isEqualTo(0xDE);
        assertThat(buf[4] & 0xFF).isEqualTo(0xAD);
        assertThat(buf[5] & 0xFF).isEqualTo(0xBE);
        assertThat(buf[6] & 0xFF).isEqualTo(0xEF);
        assertThat(ByteUtils.readInt(buf, 3)).isEqualTo(0xDEADBEEF);
    }

    @Test
    void rejectsOutOfRangeWrite() {
        byte[] buf = new byte[2];
        assertThatThrownBy(() -> ByteUtils.writeInt(buf, 0, 1))
            .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ByteUtils.writeShort(buf, 1, (short) 1))
            .isInstanceOf(IndexOutOfBoundsException.class);
        assertThatThrownBy(() -> ByteUtils.writeByte(buf, -1, (byte) 0))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void rejectsOutOfRangeRead() {
        byte[] buf = new byte[4];
        assertThatThrownBy(() -> ByteUtils.readLong(buf, 0))
            .isInstanceOf(IndexOutOfBoundsException.class);
    }

    @Test
    void intToBytesAndLongToBytes() {
        assertThat(ByteUtils.intToBytes(0x01020304))
            .containsExactly(0x01, 0x02, 0x03, 0x04);
        assertThat(ByteUtils.longToBytes(0x0102030405060708L))
            .containsExactly(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08);
    }

    @Test
    void toHexEncodesBytesLowercase() {
        assertThat(ByteUtils.toHex(new byte[]{0x00, 0x0f, (byte) 0xab, (byte) 0xff}))
            .isEqualTo("000fabff");
    }
}
