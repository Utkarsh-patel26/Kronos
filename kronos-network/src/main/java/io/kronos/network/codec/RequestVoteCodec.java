package io.kronos.network.codec;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.message.RequestVoteRequest;
import io.kronos.network.rpc.message.RequestVoteResponse;

import java.io.*;

/**
 * Binary codec for RequestVote request and response.
 *
 * Request wire layout:
 *   [8]    term
 *   [2+N]  candidateId (writeUTF)
 *   [8]    lastLogIndex
 *   [8]    lastLogTerm
 *
 * Response wire layout:
 *   [8]  term
 *   [1]  voteGranted
 */
public final class RequestVoteCodec {

    private RequestVoteCodec() {}

    public static byte[] encodeRequest(RequestVoteRequest req) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(req.term().value());
            dos.writeUTF(req.candidateId().value());
            dos.writeLong(req.lastLogIndex().value());
            dos.writeLong(req.lastLogTerm().value());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("encode RequestVoteRequest", e);
        }
    }

    public static RequestVoteRequest decodeRequest(byte[] data) {
        try {
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            Term     term         = Term.of(dis.readLong());
            NodeId   candidateId  = NodeId.of(dis.readUTF());
            LogIndex lastLogIndex = LogIndex.of(dis.readLong());
            Term     lastLogTerm  = Term.of(dis.readLong());
            return new RequestVoteRequest(term, candidateId, lastLogIndex, lastLogTerm);
        } catch (IOException e) {
            throw new UncheckedIOException("decode RequestVoteRequest", e);
        }
    }

    public static byte[] encodeResponse(RequestVoteResponse resp) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(9);
            DataOutputStream dos = new DataOutputStream(baos);
            dos.writeLong(resp.term().value());
            dos.writeBoolean(resp.voteGranted());
            dos.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException("encode RequestVoteResponse", e);
        }
    }

    public static RequestVoteResponse decodeResponse(byte[] data) {
        try {
            DataInputStream dis  = new DataInputStream(new ByteArrayInputStream(data));
            Term    term         = Term.of(dis.readLong());
            boolean voteGranted  = dis.readBoolean();
            return new RequestVoteResponse(term, voteGranted);
        } catch (IOException e) {
            throw new UncheckedIOException("decode RequestVoteResponse", e);
        }
    }
}
