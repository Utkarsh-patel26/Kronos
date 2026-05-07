package io.kronos.network.codec;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.message.AppendEntriesRequest;
import io.kronos.network.rpc.message.AppendEntriesResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.*;

class AppendEntriesCodecTest {

    @Test
    void requestRoundTripWithEntries() {
        List<LogEntry> entries = List.of(
            new LogEntry(Term.of(1), LogIndex.of(1), new byte[]{10, 20}),
            new LogEntry(Term.of(1), LogIndex.of(2), new byte[]{30, 40, 50})
        );
        AppendEntriesRequest req = new AppendEntriesRequest(
            Term.of(3),
            NodeId.of("leader-1"),
            LogIndex.of(0),
            Term.of(2),
            entries,
            LogIndex.of(2)
        );

        byte[] encoded = AppendEntriesCodec.encodeRequest(req);
        AppendEntriesRequest decoded = AppendEntriesCodec.decodeRequest(encoded);

        assertThat(decoded.term()).isEqualTo(req.term());
        assertThat(decoded.leaderId()).isEqualTo(req.leaderId());
        assertThat(decoded.prevLogIndex()).isEqualTo(req.prevLogIndex());
        assertThat(decoded.prevLogTerm()).isEqualTo(req.prevLogTerm());
        assertThat(decoded.leaderCommit()).isEqualTo(req.leaderCommit());
        assertThat(decoded.entries()).hasSize(2);
        assertThat(decoded.entries().get(0).term()).isEqualTo(Term.of(1));
        assertThat(decoded.entries().get(0).index()).isEqualTo(LogIndex.of(1));
        assertThat(decoded.entries().get(0).payload()).isEqualTo(new byte[]{10, 20});
        assertThat(decoded.entries().get(1).payload()).isEqualTo(new byte[]{30, 40, 50});
    }

    @Test
    void requestRoundTripWithNoEntries() {
        AppendEntriesRequest req = new AppendEntriesRequest(
            Term.of(5),
            NodeId.of("leader-2"),
            LogIndex.of(10),
            Term.of(4),
            List.of(),
            LogIndex.of(8)
        );

        byte[] encoded = AppendEntriesCodec.encodeRequest(req);
        AppendEntriesRequest decoded = AppendEntriesCodec.decodeRequest(encoded);

        assertThat(decoded.isHeartbeat()).isTrue();
        assertThat(decoded.term()).isEqualTo(Term.of(5));
        assertThat(decoded.leaderId()).isEqualTo(NodeId.of("leader-2"));
        assertThat(decoded.entries()).isEmpty();
    }

    @Test
    void requestWithLargePayloadRoundTrips() {
        byte[] payload = new byte[4096];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i % 256);

        AppendEntriesRequest req = new AppendEntriesRequest(
            Term.of(1), NodeId.of("n1"),
            LogIndex.ZERO, Term.ZERO,
            List.of(new LogEntry(Term.of(1), LogIndex.ONE, payload)),
            LogIndex.ONE
        );

        byte[] encoded = AppendEntriesCodec.encodeRequest(req);
        AppendEntriesRequest decoded = AppendEntriesCodec.decodeRequest(encoded);

        assertThat(decoded.entries().get(0).payload()).isEqualTo(payload);
    }

    @Test
    void responseRoundTripSuccess() {
        AppendEntriesResponse resp = new AppendEntriesResponse(Term.of(2), true, LogIndex.of(7));

        byte[] encoded = AppendEntriesCodec.encodeResponse(resp);
        AppendEntriesResponse decoded = AppendEntriesCodec.decodeResponse(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(2));
        assertThat(decoded.success()).isTrue();
        assertThat(decoded.matchIndex()).isEqualTo(LogIndex.of(7));
    }

    @Test
    void responseRoundTripFailure() {
        AppendEntriesResponse resp = new AppendEntriesResponse(Term.of(9), false, LogIndex.ZERO);

        byte[] encoded = AppendEntriesCodec.encodeResponse(resp);
        AppendEntriesResponse decoded = AppendEntriesCodec.decodeResponse(encoded);

        assertThat(decoded.success()).isFalse();
        assertThat(decoded.matchIndex()).isEqualTo(LogIndex.ZERO);
    }

    @Test
    void requestEncodingIsCompact() {
        // A heartbeat with no entries: 8+4+8+8+4+8 = 40 bytes
        // (writeUTF "n1" = 2-byte length + 2 bytes content = 4 bytes)
        AppendEntriesRequest req = new AppendEntriesRequest(
            Term.of(1), NodeId.of("n1"),
            LogIndex.ZERO, Term.ZERO,
            List.of(), LogIndex.ZERO
        );
        byte[] encoded = AppendEntriesCodec.encodeRequest(req);
        assertThat(encoded.length).isEqualTo(40);
    }

    @Test
    void responseEncodingIsExactly17Bytes() {
        AppendEntriesResponse resp = new AppendEntriesResponse(Term.ZERO, false, LogIndex.ZERO);
        assertThat(AppendEntriesCodec.encodeResponse(resp).length).isEqualTo(17);
    }
}
