package io.kronos.network.codec;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.message.AppendEntriesRequest;
import io.kronos.network.rpc.message.AppendEntriesResponse;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Binary codec for AppendEntries request and response.
 *
 * Request wire layout:
 *   [8]    term
 *   [2+N]  leaderId  (DataOutputStream.writeUTF — 2-byte length + UTF-8)
 *   [8]    prevLogIndex
 *   [8]    prevLogTerm
 *   [4]    entry count
 *   per entry:
 *     [8]  entry.term
 *     [8]  entry.index
 *     [4]  payload length
 *     [N]  payload bytes
 *   [8]    leaderCommit
 *
 * Response wire layout:
 *   [8]  term
 *   [1]  success (1=true, 0=false)
 *   [8]  matchIndex
 */
public final class AppendEntriesCodec {

    private AppendEntriesCodec() {}

    public static byte[] encodeRequest(AppendEntriesRequest req) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(req.term().value());
            dos.writeUTF(req.leaderId().value());
            dos.writeLong(req.prevLogIndex().value());
            dos.writeLong(req.prevLogTerm().value());
            dos.writeInt(req.entries().size());
            for (LogEntry e : req.entries()) {
                dos.writeLong(e.term().value());
                dos.writeLong(e.index().value());
                byte[] payload = e.payload();
                dos.writeInt(payload.length);
                dos.write(payload);
            }
            dos.writeLong(req.leaderCommit().value());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("encode AppendEntriesRequest", e);
        }
    }

    public static AppendEntriesRequest decodeRequest(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Term            term         = Term.of(dis.readLong());
            NodeId          leaderId     = NodeId.of(dis.readUTF());
            LogIndex        prevLogIndex = LogIndex.of(dis.readLong());
            Term            prevLogTerm  = Term.of(dis.readLong());
            int             count        = dis.readInt();
            List<LogEntry>  entries      = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                Term   eTerm   = Term.of(dis.readLong());
                LogIndex eIdx  = LogIndex.of(dis.readLong());
                int    pLen    = dis.readInt();
                byte[] payload = new byte[pLen];
                dis.readFully(payload);
                entries.add(new LogEntry(eTerm, eIdx, payload));
            }
            LogIndex leaderCommit = LogIndex.of(dis.readLong());
            return new AppendEntriesRequest(term, leaderId, prevLogIndex, prevLogTerm,
                                            entries, leaderCommit);
        } catch (IOException e) {
            throw new UncheckedIOException("decode AppendEntriesRequest", e);
        }
    }

    public static byte[] encodeResponse(AppendEntriesResponse resp) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(17);
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(resp.term().value());
            dos.writeBoolean(resp.success());
            dos.writeLong(resp.matchIndex().value());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("encode AppendEntriesResponse", e);
        }
    }

    public static AppendEntriesResponse decodeResponse(byte[] data) {
        try {
            DataInputStream dis   = new DataInputStream(new ByteArrayInputStream(data));
            Term     term         = Term.of(dis.readLong());
            boolean  success      = dis.readBoolean();
            LogIndex matchIndex   = LogIndex.of(dis.readLong());
            return new AppendEntriesResponse(term, success, matchIndex);
        } catch (IOException e) {
            throw new UncheckedIOException("decode AppendEntriesResponse", e);
        }
    }
}
