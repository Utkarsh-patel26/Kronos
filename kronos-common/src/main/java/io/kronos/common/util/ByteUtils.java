package io.kronos.common.util;

import java.util.Objects;

/**
 * Big-endian read/write helpers for the hand-rolled wire protocol.
 *
 * <p>All multi-byte integer values on the Kronos wire are big-endian
 * (network byte order). These helpers are the single source of truth for
 * that encoding; every later module that touches raw bytes goes through
 * this class rather than rolling its own shifts.
 */
public final class ByteUtils {

    private ByteUtils() {}

    // ---------- int16 ----------

    public static void writeShort(byte[] dst, int offset, short value) {
        checkRange(dst, offset, 2);
        dst[offset]     = (byte) ((value >>> 8) & 0xFF);
        dst[offset + 1] = (byte) (value & 0xFF);
    }

    public static short readShort(byte[] src, int offset) {
        checkRange(src, offset, 2);
        return (short) (((src[offset] & 0xFF) << 8)
                       | (src[offset + 1] & 0xFF));
    }

    // ---------- int32 ----------

    public static void writeInt(byte[] dst, int offset, int value) {
        checkRange(dst, offset, 4);
        dst[offset]     = (byte) ((value >>> 24) & 0xFF);
        dst[offset + 1] = (byte) ((value >>> 16) & 0xFF);
        dst[offset + 2] = (byte) ((value >>> 8)  & 0xFF);
        dst[offset + 3] = (byte) (value & 0xFF);
    }

    public static int readInt(byte[] src, int offset) {
        checkRange(src, offset, 4);
        return  ((src[offset]     & 0xFF) << 24)
              | ((src[offset + 1] & 0xFF) << 16)
              | ((src[offset + 2] & 0xFF) << 8)
              |  (src[offset + 3] & 0xFF);
    }

    // ---------- int64 ----------

    public static void writeLong(byte[] dst, int offset, long value) {
        checkRange(dst, offset, 8);
        dst[offset]     = (byte) ((value >>> 56) & 0xFF);
        dst[offset + 1] = (byte) ((value >>> 48) & 0xFF);
        dst[offset + 2] = (byte) ((value >>> 40) & 0xFF);
        dst[offset + 3] = (byte) ((value >>> 32) & 0xFF);
        dst[offset + 4] = (byte) ((value >>> 24) & 0xFF);
        dst[offset + 5] = (byte) ((value >>> 16) & 0xFF);
        dst[offset + 6] = (byte) ((value >>> 8)  & 0xFF);
        dst[offset + 7] = (byte) (value & 0xFF);
    }

    public static long readLong(byte[] src, int offset) {
        checkRange(src, offset, 8);
        return  ((long) (src[offset]     & 0xFF) << 56)
              | ((long) (src[offset + 1] & 0xFF) << 48)
              | ((long) (src[offset + 2] & 0xFF) << 40)
              | ((long) (src[offset + 3] & 0xFF) << 32)
              | ((long) (src[offset + 4] & 0xFF) << 24)
              | ((long) (src[offset + 5] & 0xFF) << 16)
              | ((long) (src[offset + 6] & 0xFF) << 8)
              |  ((long) src[offset + 7] & 0xFF);
    }

    // ---------- float / double ----------

    public static void writeFloat(byte[] dst, int offset, float value) {
        writeInt(dst, offset, Float.floatToRawIntBits(value));
    }

    public static float readFloat(byte[] src, int offset) {
        return Float.intBitsToFloat(readInt(src, offset));
    }

    public static void writeDouble(byte[] dst, int offset, double value) {
        writeLong(dst, offset, Double.doubleToRawLongBits(value));
    }

    public static double readDouble(byte[] src, int offset) {
        return Double.longBitsToDouble(readLong(src, offset));
    }

    // ---------- bytes ----------

    public static void writeByte(byte[] dst, int offset, byte value) {
        checkRange(dst, offset, 1);
        dst[offset] = value;
    }

    public static byte readByte(byte[] src, int offset) {
        checkRange(src, offset, 1);
        return src[offset];
    }

    public static void writeBoolean(byte[] dst, int offset, boolean value) {
        writeByte(dst, offset, value ? (byte) 1 : (byte) 0);
    }

    public static boolean readBoolean(byte[] src, int offset) {
        return readByte(src, offset) != 0;
    }

    // ---------- helpers ----------

    private static void checkRange(byte[] buf, int offset, int length) {
        Objects.requireNonNull(buf, "buffer");
        if (offset < 0 || length < 0 || offset + length > buf.length) {
            throw new IndexOutOfBoundsException(
                "offset=" + offset + " length=" + length + " bufLen=" + buf.length);
        }
    }

    /** Convert an int to its 4-byte big-endian representation (handy for tests). */
    public static byte[] intToBytes(int value) {
        byte[] out = new byte[4];
        writeInt(out, 0, value);
        return out;
    }

    /** Convert a long to its 8-byte big-endian representation (handy for tests). */
    public static byte[] longToBytes(long value) {
        byte[] out = new byte[8];
        writeLong(out, 0, value);
        return out;
    }

    /** Hex dump — used by the logger when rendering raw frames. */
    public static String toHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(Character.forDigit((b >> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
