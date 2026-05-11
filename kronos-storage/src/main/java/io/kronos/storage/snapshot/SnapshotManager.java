package io.kronos.storage.snapshot;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.zip.CRC32;

import static java.nio.file.StandardOpenOption.*;

/**
 * Manages snapshot files on disk.
 *
 * <p>Snapshot file format:
 * <pre>
 * HEADER (48 bytes): [8] magic  [8] lastIncludedIndex  [8] lastIncludedTerm
 *                    [8] bodyLength  [8] createdAt  [8] reserved
 * BODY (bodyLength bytes): serialized state-machine state
 * FOOTER (4 bytes): CRC32 of header + body
 * </pre>
 * Files are atomically renamed from a temp file so partial writes are never visible.
 */
public final class SnapshotManager {

    private static final long   MAGIC       = 0x534E415053484F54L; // "SNAPSHOT"
    private static final int    HEADER_SIZE = 48;
    private static final String SNAP_PREFIX = "snapshot-";
    private static final String SNAP_SUFFIX = ".snap";

    private final Path snapshotDir;
    /** Temp file used when assembling an incoming chunked snapshot from a leader. */
    private Path chunkedTempFile;

    public SnapshotManager(Path snapshotDir) throws IOException {
        this.snapshotDir = snapshotDir;
        Files.createDirectories(snapshotDir);
    }

    /**
     * Write a complete snapshot to disk atomically.
     * Deletes all previous snapshot files after the new one is in place.
     */
    public SnapshotMetadata save(LogIndex lastIndex, Term lastTerm, byte[] state) throws IOException {
        Path tmp = snapshotDir.resolve("snapshot.tmp");

        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
        header.putLong(MAGIC);
        header.putLong(lastIndex.value());
        header.putLong(lastTerm.value());
        header.putLong(state.length);
        header.putLong(System.currentTimeMillis());
        header.putLong(0L);
        header.flip();

        CRC32 crc = new CRC32();
        crc.update(header.array(), 0, HEADER_SIZE);
        crc.update(state);

        try (FileChannel fc = FileChannel.open(tmp, WRITE, CREATE, TRUNCATE_EXISTING)) {
            writeAll(fc, header);
            writeAll(fc, ByteBuffer.wrap(state));
            ByteBuffer footer = ByteBuffer.allocate(4);
            footer.putInt((int) crc.getValue());
            footer.flip();
            writeAll(fc, footer);
            fc.force(true);
        }

        Path finalPath = snapshotDir.resolve(
            String.format("%s%08d-term-%d%s",
                SNAP_PREFIX, lastIndex.value(), lastTerm.value(), SNAP_SUFFIX));
        Files.move(tmp, finalPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);

        deleteOldSnapshots(finalPath);
        return new SnapshotMetadata(lastIndex, lastTerm, finalPath);
    }

    /** Returns the latest snapshot metadata, or {@code null} if no snapshot exists. */
    public SnapshotMetadata latestSnapshot() throws IOException {
        try (Stream<Path> stream = Files.list(snapshotDir)) {
            Optional<Path> latest = stream
                .filter(p -> p.getFileName().toString().startsWith(SNAP_PREFIX)
                          && p.getFileName().toString().endsWith(SNAP_SUFFIX))
                .max(Comparator.comparing(p -> p.getFileName().toString()));
            return latest.map(this::readMetadata).orElse(null);
        }
    }

    /**
     * Load and return the state bytes from a snapshot file, verifying its CRC.
     * Throws {@link CorruptedSnapshotException} if the CRC does not match.
     */
    public byte[] loadState(SnapshotMetadata meta) throws IOException {
        try (FileChannel fc = FileChannel.open(meta.path(), READ)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            readAll(fc, header);
            header.flip();

            long magic = header.getLong();
            if (magic != MAGIC) {
                throw new CorruptedSnapshotException("bad magic in " + meta.path().getFileName());
            }
            header.getLong(); // lastIncludedIndex — already in meta
            header.getLong(); // lastIncludedTerm  — already in meta
            long bodyLen = header.getLong();
            if (bodyLen < 0 || bodyLen > Integer.MAX_VALUE) {
                throw new CorruptedSnapshotException("invalid body length in " + meta.path().getFileName());
            }

            byte[] state = new byte[(int) bodyLen];
            readAll(fc, ByteBuffer.wrap(state));

            ByteBuffer footer = ByteBuffer.allocate(4);
            readAll(fc, footer);
            footer.flip();
            int storedCrc = footer.getInt();

            CRC32 crc = new CRC32();
            crc.update(header.array(), 0, HEADER_SIZE);
            crc.update(state);
            if ((int) crc.getValue() != storedCrc) {
                throw new CorruptedSnapshotException("CRC32 mismatch in " + meta.path().getFileName());
            }
            return state;
        }
    }

    /** Read the raw bytes of a snapshot file (used when streaming the snapshot to a lagging follower). */
    public byte[] readRaw(SnapshotMetadata meta) throws IOException {
        return Files.readAllBytes(meta.path());
    }

    /**
     * Write one chunk of an incoming snapshot from a leader.
     * An offset of 0 starts a fresh temp file; subsequent offsets append to it.
     */
    public void writeChunk(int offset, byte[] data) throws IOException {
        if (offset == 0) {
            chunkedTempFile = snapshotDir.resolve("incoming.tmp");
            Files.deleteIfExists(chunkedTempFile);
        }
        try (FileChannel fc = FileChannel.open(chunkedTempFile, WRITE, CREATE, APPEND)) {
            writeAll(fc, ByteBuffer.wrap(data));
        }
    }

    /**
     * Finalize an incoming chunked snapshot: validate CRC, atomically rename to final name.
     * Returns the metadata of the installed snapshot.
     */
    public SnapshotMetadata finalizeChunked(LogIndex lastIndex, Term lastTerm) throws IOException {
        if (chunkedTempFile == null || !Files.exists(chunkedTempFile)) {
            throw new IOException("no chunked snapshot assembly in progress");
        }

        // Validate by loading state (also verifies CRC)
        SnapshotMetadata tmpMeta = new SnapshotMetadata(lastIndex, lastTerm, chunkedTempFile);
        loadState(tmpMeta); // throws CorruptedSnapshotException if invalid

        Path finalPath = snapshotDir.resolve(
            String.format("%s%08d-term-%d%s",
                SNAP_PREFIX, lastIndex.value(), lastTerm.value(), SNAP_SUFFIX));
        Files.move(chunkedTempFile, finalPath,
            StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        chunkedTempFile = null;

        deleteOldSnapshots(finalPath);
        return new SnapshotMetadata(lastIndex, lastTerm, finalPath);
    }

    // -----------------------------------------------------------------------

    private SnapshotMetadata readMetadata(Path path) {
        try (FileChannel fc = FileChannel.open(path, READ)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            readAll(fc, header);
            header.flip();
            header.getLong(); // magic
            long indexVal = header.getLong();
            long termVal  = header.getLong();
            return new SnapshotMetadata(LogIndex.of(indexVal), Term.of(termVal), path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void deleteOldSnapshots(Path keepPath) throws IOException {
        try (Stream<Path> stream = Files.list(snapshotDir)) {
            stream
                .filter(p -> p.getFileName().toString().startsWith(SNAP_PREFIX)
                          && p.getFileName().toString().endsWith(SNAP_SUFFIX)
                          && !p.equals(keepPath))
                .forEach(p -> {
                    try { Files.deleteIfExists(p); } catch (IOException ignored) {}
                });
        }
    }

    private static void writeAll(FileChannel fc, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) fc.write(buf);
    }

    private static void readAll(FileChannel fc, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int n = fc.read(buf);
            if (n == -1) break;
        }
    }
}
