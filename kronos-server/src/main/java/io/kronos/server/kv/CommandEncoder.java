package io.kronos.server.kv;

import java.io.*;

/**
 * Encodes client KV operations into the compact binary format stored in Raft log entries.
 *
 * Wire layout: [1 byte type] [key as writeUTF] [4-byte value length] [value bytes]
 * DELETE omits the value section; CAS writes expected then new value, each length-prefixed.
 */
public final class CommandEncoder {

    static final byte PUT    = 0x01;
    static final byte DELETE = 0x02;
    static final byte CAS    = 0x03;

    private CommandEncoder() {}

    public static byte[] encodePut(String key, byte[] value) {
        return encode(dos -> {
            dos.writeByte(PUT);
            dos.writeUTF(key);
            dos.writeInt(value.length);
            dos.write(value);
        });
    }

    public static byte[] encodeDelete(String key) {
        return encode(dos -> {
            dos.writeByte(DELETE);
            dos.writeUTF(key);
        });
    }

    public static byte[] encodeCas(String key, byte[] expected, byte[] newValue) {
        return encode(dos -> {
            dos.writeByte(CAS);
            dos.writeUTF(key);
            dos.writeInt(expected.length);
            dos.write(expected);
            dos.writeInt(newValue.length);
            dos.write(newValue);
        });
    }

    @FunctionalInterface
    private interface IoWriter {
        void write(DataOutputStream dos) throws IOException;
    }

    private static byte[] encode(IoWriter writer) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream dos = new DataOutputStream(baos)) {
            writer.write(dos);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return baos.toByteArray();
    }
}
