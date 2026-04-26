package io.kronos.common.util;

import java.util.zip.CRC32;

/**
 * Thin wrapper around {@link CRC32} so callers don't need to manage
 * instances themselves. Used by the WAL and the wire protocol to detect
 * corruption; neither is the CRC intended as a security primitive.
 */
public final class CRC32Util {

    private CRC32Util() {}

    public static long compute(byte[] data) {
        return compute(data, 0, data.length);
    }

    public static long compute(byte[] data, int offset, int length) {
        CRC32 crc = new CRC32();
        crc.update(data, offset, length);
        return crc.getValue();
    }

    /** Verify that {@code expected} matches the CRC of {@code data}. */
    public static boolean verify(byte[] data, long expected) {
        return compute(data) == expected;
    }
}
