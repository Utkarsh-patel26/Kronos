package io.kronos.network.codec;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.message.InstallSnapshotRequest;
import io.kronos.network.rpc.message.InstallSnapshotResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class InstallSnapshotCodecTest {

    @Test
    void requestRoundTripFinalChunk() {
        byte[] snapshotData = {1, 2, 3, 4, 5};
        InstallSnapshotRequest req = new InstallSnapshotRequest(
            Term.of(4),
            NodeId.of("leader-1"),
            LogIndex.of(100),
            Term.of(3),
            0,
            snapshotData,
            true
        );

        byte[] encoded = InstallSnapshotCodec.encodeRequest(req);
        InstallSnapshotRequest decoded = InstallSnapshotCodec.decodeRequest(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(4));
        assertThat(decoded.leaderId()).isEqualTo(NodeId.of("leader-1"));
        assertThat(decoded.lastIncludedIndex()).isEqualTo(LogIndex.of(100));
        assertThat(decoded.lastIncludedTerm()).isEqualTo(Term.of(3));
        assertThat(decoded.offset()).isEqualTo(0);
        assertThat(decoded.data()).isEqualTo(snapshotData);
        assertThat(decoded.done()).isTrue();
    }

    @Test
    void requestRoundTripIntermediateChunk() {
        byte[] chunk = new byte[1024];
        for (int i = 0; i < chunk.length; i++) chunk[i] = (byte) i;

        InstallSnapshotRequest req = new InstallSnapshotRequest(
            Term.of(2),
            NodeId.of("n0"),
            LogIndex.of(50),
            Term.of(2),
            4096,
            chunk,
            false
        );

        byte[] encoded = InstallSnapshotCodec.encodeRequest(req);
        InstallSnapshotRequest decoded = InstallSnapshotCodec.decodeRequest(encoded);

        assertThat(decoded.offset()).isEqualTo(4096);
        assertThat(decoded.done()).isFalse();
        assertThat(decoded.data()).isEqualTo(chunk);
    }

    @Test
    void requestWithEmptyDataRoundTrips() {
        InstallSnapshotRequest req = new InstallSnapshotRequest(
            Term.of(1), NodeId.of("n1"),
            LogIndex.of(1), Term.of(1),
            0, new byte[0], true
        );

        byte[] encoded = InstallSnapshotCodec.encodeRequest(req);
        InstallSnapshotRequest decoded = InstallSnapshotCodec.decodeRequest(encoded);

        assertThat(decoded.data()).isEmpty();
        assertThat(decoded.done()).isTrue();
    }

    @Test
    void responseRoundTrip() {
        InstallSnapshotResponse resp = new InstallSnapshotResponse(Term.of(6));

        byte[] encoded = InstallSnapshotCodec.encodeResponse(resp);
        InstallSnapshotResponse decoded = InstallSnapshotCodec.decodeResponse(encoded);

        assertThat(decoded.term()).isEqualTo(Term.of(6));
    }

    @Test
    void responseEncodingIs8Bytes() {
        InstallSnapshotResponse resp = new InstallSnapshotResponse(Term.of(1));
        assertThat(InstallSnapshotCodec.encodeResponse(resp).length).isEqualTo(8);
    }

    @Test
    void dataMutationDoesNotAffectEncodedResult() {
        byte[] original = {1, 2, 3};
        InstallSnapshotRequest req = new InstallSnapshotRequest(
            Term.of(1), NodeId.of("n1"),
            LogIndex.ONE, Term.of(1),
            0, original, true
        );
        // Mutate original after construction
        original[0] = 99;

        byte[] encoded = InstallSnapshotCodec.encodeRequest(req);
        InstallSnapshotRequest decoded = InstallSnapshotCodec.decodeRequest(encoded);

        // InstallSnapshotRequest clones data in constructor — mutation must not bleed through
        assertThat(decoded.data()[0]).isEqualTo((byte) 1);
    }
}
