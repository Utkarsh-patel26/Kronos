package io.kronos.network.codec;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.message.RequestVoteRequest;
import io.kronos.network.rpc.message.RequestVoteResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RequestVoteCodecTest {

    @Test
    void requestRoundTripVoteGranted() {
        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(3),
            NodeId.of("candidate-1"),
            LogIndex.of(5),
            Term.of(2)
        );

        byte[] encoded = RequestVoteCodec.encodeRequest(req);
        RequestVoteRequest decoded = RequestVoteCodec.decodeRequest(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(3));
        assertThat(decoded.candidateId()).isEqualTo(NodeId.of("candidate-1"));
        assertThat(decoded.lastLogIndex()).isEqualTo(LogIndex.of(5));
        assertThat(decoded.lastLogTerm()).isEqualTo(Term.of(2));
    }

    @Test
    void requestRoundTripFirstElection() {
        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(1),
            NodeId.of("node-a"),
            LogIndex.ZERO,
            Term.ZERO
        );

        byte[] encoded = RequestVoteCodec.encodeRequest(req);
        RequestVoteRequest decoded = RequestVoteCodec.decodeRequest(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(1));
        assertThat(decoded.lastLogIndex()).isEqualTo(LogIndex.ZERO);
        assertThat(decoded.lastLogTerm()).isEqualTo(Term.ZERO);
    }

    @Test
    void responseRoundTripGranted() {
        RequestVoteResponse resp = new RequestVoteResponse(Term.of(3), true);

        byte[] encoded = RequestVoteCodec.encodeResponse(resp);
        RequestVoteResponse decoded = RequestVoteCodec.decodeResponse(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(3));
        assertThat(decoded.voteGranted()).isTrue();
    }

    @Test
    void responseRoundTripDenied() {
        RequestVoteResponse resp = new RequestVoteResponse(Term.of(7), false);

        byte[] encoded = RequestVoteCodec.encodeResponse(resp);
        RequestVoteResponse decoded = RequestVoteCodec.decodeResponse(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(7));
        assertThat(decoded.voteGranted()).isFalse();
    }

    @Test
    void requestEncodingContainsCandidateId() {
        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(1), NodeId.of("abc"),
            LogIndex.ZERO, Term.ZERO
        );
        byte[] encoded = RequestVoteCodec.encodeRequest(req);
        // Verify "abc" is somewhere in the encoded bytes
        String all = new String(encoded);
        assertThat(all).contains("abc");
    }

    @Test
    void responseEncodingIs9Bytes() {
        RequestVoteResponse resp = new RequestVoteResponse(Term.of(1), true);
        assertThat(RequestVoteCodec.encodeResponse(resp).length).isEqualTo(9);
    }

    @Test
    void requestEncodingSize() {
        // 8 (term) + 4 (writeUTF "n1": 2-byte length + 2 bytes content) + 8 + 8 = 28
        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(1), NodeId.of("n1"), LogIndex.ZERO, Term.ZERO
        );
        byte[] encoded = RequestVoteCodec.encodeRequest(req);
        assertThat(encoded.length).isEqualTo(28);
    }
}
