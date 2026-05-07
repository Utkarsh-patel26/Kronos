package io.kronos.network.codec;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.message.InstallSnapshotRequest;
import io.kronos.network.rpc.message.InstallSnapshotResponse;

import java.io.*;

/**
 * Binary codec for InstallSnapshot request and response.
 *
 * Request wire layout:
 *   [8]    term
 *   [2+N]  leaderId (writeUTF)
 *   [8]    lastIncludedIndex
 *   [8]    lastIncludedTerm
 *   [4]    offset
 *   [4]    data length
 *   [N]    data bytes
 *   [1]    done
 *
 * Response wire layout:
 *   [8]  term
 */
public final class InstallSnapshotCodec {

    private InstallSnapshotCodec() {}

    public static byte[] encodeRequest(InstallSnapshotRequest req) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(req.term().value());
            dos.writeUTF(req.leaderId().value());
            dos.writeLong(req.lastIncludedIndex().value());
            dos.writeLong(req.lastIncludedTerm().value());
            dos.writeInt(req.offset());
            byte[] data = req.data();
            dos.writeInt(data.length);
            dos.write(data);
            dos.writeBoolean(req.done());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("encode InstallSnapshotRequest", e);
        }
    }

    public static InstallSnapshotRequest decodeRequest(byte[] raw) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(raw));
            Term     term              = Term.of(dis.readLong());
            NodeId   leaderId          = NodeId.of(dis.readUTF());
            LogIndex lastIncludedIndex = LogIndex.of(dis.readLong());
            Term     lastIncludedTerm  = Term.of(dis.readLong());
            int      offset            = dis.readInt();
            int      dataLen           = dis.readInt();
            byte[]   data              = new byte[dataLen];
            dis.readFully(data);
            boolean  done              = dis.readBoolean();
            return new InstallSnapshotRequest(term, leaderId, lastIncludedIndex,
                                              lastIncludedTerm, offset, data, done);
        } catch (IOException e) {
            throw new UncheckedIOException("decode InstallSnapshotRequest", e);
        }
    }

    public static byte[] encodeResponse(InstallSnapshotResponse resp) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(8);
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(resp.term().value());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("encode InstallSnapshotResponse", e);
        }
    }

    public static InstallSnapshotResponse decodeResponse(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Term term = Term.of(dis.readLong());
            return new InstallSnapshotResponse(term);
        } catch (IOException e) {
            throw new UncheckedIOException("decode InstallSnapshotResponse", e);
        }
    }
}
