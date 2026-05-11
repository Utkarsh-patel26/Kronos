package io.kronos.server.kv;

import io.kronos.common.model.LogIndex;
import io.kronos.raft.core.SnapshotableStateMachine;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Key-value state machine applied to each committed Raft log entry.
 *
 * The store is a {@link ConcurrentHashMap} so HTTP-thread reads (GET) are safe
 * without holding the Raft lock.  Writes happen only from the Raft thread via
 * {@link #apply}.
 */
public final class KvStateMachine implements SnapshotableStateMachine {

    private final ConcurrentHashMap<String, byte[]> store = new ConcurrentHashMap<>();

    @Override
    public void apply(LogIndex index, byte[] command) {
        if (command == null || command.length == 0) return;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(command));
            byte type = in.readByte();
            switch (type) {
                case CommandEncoder.PUT -> {
                    String key   = in.readUTF();
                    byte[] value = in.readNBytes(in.readInt());
                    store.put(key, value);
                }
                case CommandEncoder.DELETE -> {
                    store.remove(in.readUTF());
                }
                case CommandEncoder.CAS -> {
                    String key      = in.readUTF();
                    byte[] expected = in.readNBytes(in.readInt());
                    byte[] newVal   = in.readNBytes(in.readInt());
                    store.compute(key, (k, current) ->
                        Arrays.equals(current, expected) ? newVal : current);
                }
                default -> { /* unknown type — ignore */ }
            }
        } catch (IOException e) {
            // malformed command — log entries must never be malformed in practice
        }
    }

    /** Thread-safe read from the HTTP API thread. */
    public byte[] get(String key) { return store.get(key); }

    public int size() { return store.size(); }

    // -----------------------------------------------------------------------
    // Snapshot protocol
    // -----------------------------------------------------------------------

    @Override
    public byte[] takeSnapshot() {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            Map<String, byte[]> snapshot = Map.copyOf(store);
            dos.writeInt(snapshot.size());
            for (Map.Entry<String, byte[]> e : snapshot.entrySet()) {
                dos.writeUTF(e.getKey());
                dos.writeInt(e.getValue().length);
                dos.write(e.getValue());
            }
            dos.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot serialization failed", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void installSnapshot(byte[] snapshot) {
        store.clear();
        if (snapshot == null || snapshot.length == 0) return;
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(snapshot));
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String key   = in.readUTF();
                byte[] value = in.readNBytes(in.readInt());
                store.put(key, value);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("snapshot restore failed", e);
        }
    }
}
