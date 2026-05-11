package io.kronos.storage.wal;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.Term;
import io.kronos.common.util.CRC32Util;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;

import static java.nio.file.StandardOpenOption.*;

/**
 * Append-only write-ahead log stored as a series of segment files.
 *
 * <p>File format per segment:
 * <pre>
 * HEADER (32 bytes): [8] magic  [8] firstIndex  [8] createdAt  [8] reserved
 * RECORD (repeated): [4] recLen  [8] term  [8] index  [4] pLen  [N] payload  [4] CRC32
 * </pre>
 * CRC32 is computed over the concatenation of term + index + payload bytes.
 */
public final class WriteAheadLog implements Closeable {

    private static final long   MAGIC             = 0x4B524F4E4F530000L;
    private static final int    HEADER_SIZE       = 32;
    private static final int    MAX_SEGMENT_BYTES = 64 * 1024 * 1024;
    private static final String SEGMENT_PREFIX    = "segment-";
    private static final String SEGMENT_SUFFIX    = ".wal";

    private final Path walDir;
    /** firstIndex → segment file path, sorted ascending. */
    private final TreeMap<Long, Path> segmentMap = new TreeMap<>();

    private FileChannel activeChannel;

    public WriteAheadLog(Path walDir) throws IOException {
        this.walDir = walDir;
        Files.createDirectories(walDir);
        loadExistingSegments();
        openOrCreateActiveSegment();
    }

    /** Append one entry durably. Rotates to a new segment when the current one is full. */
    public void persist(LogEntry entry) throws IOException {
        byte[] payload = entry.payload();
        // recLen covers: term(8) + index(8) + payloadLen(4) + payload(N) + CRC(4)
        int recLen = 8 + 8 + 4 + payload.length + 4;

        ByteBuffer crcBuf = ByteBuffer.allocate(8 + 8 + payload.length);
        crcBuf.putLong(entry.term().value());
        crcBuf.putLong(entry.index().value());
        crcBuf.put(payload);
        int crc = (int) CRC32Util.compute(crcBuf.array());

        ByteBuffer buf = ByteBuffer.allocate(4 + recLen);
        buf.putInt(recLen);
        buf.putLong(entry.term().value());
        buf.putLong(entry.index().value());
        buf.putInt(payload.length);
        buf.put(payload);
        buf.putInt(crc);
        buf.flip();

        writeAll(activeChannel, buf);
        activeChannel.force(false);

        if (activeChannel.size() >= MAX_SEGMENT_BYTES) {
            rotateSegment(entry.index().increment());
        }
    }

    /** Replay all WAL segments and return entries in index order. */
    public List<LogEntry> recover() throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        for (Map.Entry<Long, Path> seg : segmentMap.entrySet()) {
            entries.addAll(replaySegment(seg.getValue()));
        }
        return entries;
    }

    /**
     * Remove all entries with index >= from.
     * Rewrites the segment containing {@code from}; deletes all later segments.
     */
    public void truncateFrom(LogIndex from) throws IOException {
        long fromVal = from.value();
        Long segKey = segmentMap.floorKey(fromVal);
        if (segKey == null) return;

        // Delete segments strictly after the containing segment
        List<Long> toDelete = new ArrayList<>(segmentMap.tailMap(segKey, false).keySet());
        for (Long key : toDelete) {
            Files.deleteIfExists(segmentMap.remove(key));
        }

        // Rewrite the segment that contains fromVal, keeping entries before it
        Path segPath = segmentMap.get(segKey);
        List<LogEntry> kept = new ArrayList<>();
        for (LogEntry e : replaySegment(segPath)) {
            if (e.index().value() < fromVal) kept.add(e);
        }
        rewriteSegment(segPath, segKey, kept);

        if (activeChannel != null) activeChannel.close();
        activeChannel = FileChannel.open(segPath, WRITE, APPEND);
    }

    /**
     * Delete segments whose entries are entirely before {@code snapshotLastIndex}.
     * The segment that contains the snapshot boundary is kept for WAL continuity.
     */
    public void deleteSegmentsBefore(LogIndex snapshotLastIndex) throws IOException {
        long snapVal  = snapshotLastIndex.value();
        Long keepFrom = segmentMap.floorKey(snapVal);
        if (keepFrom == null) return;

        List<Long> toDelete = new ArrayList<>(segmentMap.headMap(keepFrom, false).keySet());
        for (Long key : toDelete) {
            Files.deleteIfExists(segmentMap.remove(key));
        }
    }

    @Override
    public void close() throws IOException {
        if (activeChannel != null) {
            activeChannel.close();
            activeChannel = null;
        }
    }

    // -----------------------------------------------------------------------

    private void loadExistingSegments() throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(
                walDir, SEGMENT_PREFIX + "*" + SEGMENT_SUFFIX)) {
            for (Path seg : ds) {
                long firstIdx = readSegmentFirstIndex(seg);
                segmentMap.put(firstIdx, seg);
            }
        }
    }

    private long readSegmentFirstIndex(Path seg) throws IOException {
        try (FileChannel fc = FileChannel.open(seg, READ)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            readAll(fc, header);
            header.flip();
            long magic = header.getLong();
            if (magic != MAGIC) {
                throw new CorruptedWalException("bad WAL magic in " + seg.getFileName());
            }
            return header.getLong(); // firstIndex
        }
    }

    private void openOrCreateActiveSegment() throws IOException {
        if (segmentMap.isEmpty()) {
            createSegment(1L);
        } else {
            Map.Entry<Long, Path> last = segmentMap.lastEntry();
            activeChannel = FileChannel.open(last.getValue(), WRITE, APPEND);
        }
    }

    private void createSegment(long firstIndex) throws IOException {
        Path segPath = walDir.resolve(
            SEGMENT_PREFIX + String.format("%016d", firstIndex) + SEGMENT_SUFFIX);
        try (FileChannel fc = FileChannel.open(segPath, WRITE, CREATE, TRUNCATE_EXISTING)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            header.putLong(MAGIC);
            header.putLong(firstIndex);
            header.putLong(System.currentTimeMillis());
            header.putLong(0L);
            header.flip();
            writeAll(fc, header);
            fc.force(true);
        }
        segmentMap.put(firstIndex, segPath);
        activeChannel = FileChannel.open(segPath, WRITE, APPEND);
    }

    private void rotateSegment(LogIndex nextFirstIndex) throws IOException {
        activeChannel.close();
        createSegment(nextFirstIndex.value());
    }

    private List<LogEntry> replaySegment(Path seg) throws IOException {
        List<LogEntry> entries = new ArrayList<>();
        try (FileChannel fc = FileChannel.open(seg, READ)) {
            fc.position(HEADER_SIZE);
            ByteBuffer lenBuf = ByteBuffer.allocate(4);
            LogEntry entry;
            while ((entry = readNextRecord(fc, lenBuf, seg)) != null) {
                entries.add(entry);
            }
        }
        return entries;
    }

    private LogEntry readNextRecord(FileChannel fc, ByteBuffer lenBuf, Path seg) throws IOException {
        lenBuf.clear();
        if (readAll(fc, lenBuf) < 4) return null;
        lenBuf.flip();
        int recLen = lenBuf.getInt();
        if (recLen <= 0) return null;

        ByteBuffer rec = ByteBuffer.allocate(recLen);
        if (readAll(fc, rec) < recLen) return null; // truncated record

        rec.flip();
        long   termVal  = rec.getLong();
        long   indexVal = rec.getLong();
        int    pLen     = rec.getInt();
        if (pLen < 0 || pLen > recLen) return null;

        byte[] payload = new byte[pLen];
        rec.get(payload);
        int storedCrc = rec.getInt();

        ByteBuffer crcBuf = ByteBuffer.allocate(8 + 8 + pLen);
        crcBuf.putLong(termVal);
        crcBuf.putLong(indexVal);
        crcBuf.put(payload);
        int computed = (int) CRC32Util.compute(crcBuf.array());
        if (computed != storedCrc) {
            throw new CorruptedWalException(
                "CRC32 mismatch at index " + indexVal + " in " + seg.getFileName());
        }
        return new LogEntry(Term.of(termVal), LogIndex.of(indexVal), payload);
    }

    private void rewriteSegment(Path seg, long firstIndex, List<LogEntry> entries) throws IOException {
        Path tmp = seg.resolveSibling(seg.getFileName() + ".tmp");
        try (FileChannel fc = FileChannel.open(tmp, WRITE, CREATE, TRUNCATE_EXISTING)) {
            ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
            header.putLong(MAGIC);
            header.putLong(firstIndex);
            header.putLong(System.currentTimeMillis());
            header.putLong(0L);
            header.flip();
            writeAll(fc, header);

            for (LogEntry e : entries) {
                byte[] payload = e.payload();
                int recLen = 8 + 8 + 4 + payload.length + 4;

                ByteBuffer crcBuf = ByteBuffer.allocate(8 + 8 + payload.length);
                crcBuf.putLong(e.term().value());
                crcBuf.putLong(e.index().value());
                crcBuf.put(payload);
                int crc = (int) CRC32Util.compute(crcBuf.array());

                ByteBuffer buf = ByteBuffer.allocate(4 + recLen);
                buf.putInt(recLen);
                buf.putLong(e.term().value());
                buf.putLong(e.index().value());
                buf.putInt(payload.length);
                buf.put(payload);
                buf.putInt(crc);
                buf.flip();
                writeAll(fc, buf);
            }
            fc.force(true);
        }
        Files.move(tmp, seg, StandardCopyOption.REPLACE_EXISTING);
    }

    private static void writeAll(FileChannel fc, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) fc.write(buf);
    }

    private static int readAll(FileChannel fc, ByteBuffer buf) throws IOException {
        int total = 0;
        while (buf.hasRemaining()) {
            int n = fc.read(buf);
            if (n == -1) break;
            total += n;
        }
        return total;
    }
}
