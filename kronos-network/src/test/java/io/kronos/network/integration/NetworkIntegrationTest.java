package io.kronos.network.integration;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.channel.NioServer;
import io.kronos.network.codec.AppendEntriesCodec;
import io.kronos.network.codec.InstallSnapshotCodec;
import io.kronos.network.codec.RequestVoteCodec;
import io.kronos.network.protocol.*;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.network.rpc.message.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end tests: two NioServer instances exchange all six message types
 * and verify correctness of the full framing + codec + RPC stack.
 */
class NetworkIntegrationTest {

    private NioServer        serverA;
    private NioServer        serverB;
    private RpcDispatcher    dispatcherA;
    private Thread           threadA;
    private Thread           threadB;
    private ExecutorService  executor;
    private ScheduledExecutorService scheduler;

    @BeforeEach
    void setUp() throws IOException {
        executor  = Executors.newCachedThreadPool();
        scheduler = Executors.newSingleThreadScheduledExecutor();

        dispatcherA = new RpcDispatcher(executor);
        serverA = new NioServer("127.0.0.1", 0, dispatcherA::dispatch);
        dispatcherA.setWriter(serverA::enqueueWrite);

        serverB = new NioServer("127.0.0.1", 0, (f, ctx) -> {});

        threadA = new Thread(serverA, "srv-a");
        threadB = new Thread(serverB, "srv-b");
        threadA.setDaemon(true);
        threadB.setDaemon(true);
        threadA.start();
        threadB.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        serverA.close();
        serverB.close();
        threadA.interrupt();
        threadB.interrupt();
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    // -----------------------------------------------------------------------
    // RequestVote round-trip
    // -----------------------------------------------------------------------

    @Test
    void requestVoteRoundTrip() throws Exception {
        dispatcherA.register(MessageType.REQUEST_VOTE_REQUEST, body -> {
            RequestVoteRequest req = RequestVoteCodec.decodeRequest(body);
            boolean grant = req.term().value() >= 1;
            return RequestVoteCodec.encodeResponse(new RequestVoteResponse(req.term(), grant));
        });

        RpcChannel ch = connectChannel(NodeId.of("a"), serverA.localPort());

        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(3), NodeId.of("b"), LogIndex.of(5), Term.of(2));
        RequestVoteResponse resp = ch.send(
            MessageType.REQUEST_VOTE_REQUEST,
            RequestVoteCodec.encodeRequest(req),
            frame -> RequestVoteCodec.decodeResponse(frame.body())
        ).get(3, TimeUnit.SECONDS);

        assertThat(resp.voteGranted()).isTrue();
        assertThat(resp.term()).isEqualTo(Term.of(3));
    }

    // -----------------------------------------------------------------------
    // AppendEntries round-trip
    // -----------------------------------------------------------------------

    @Test
    void appendEntriesHeartbeatRoundTrip() throws Exception {
        dispatcherA.register(MessageType.APPEND_ENTRIES_REQUEST, body -> {
            AppendEntriesRequest req = AppendEntriesCodec.decodeRequest(body);
            assertThat(req.isHeartbeat()).isTrue();
            return AppendEntriesCodec.encodeResponse(
                new AppendEntriesResponse(req.term(), true, req.leaderCommit()));
        });

        RpcChannel ch = connectChannel(NodeId.of("a"), serverA.localPort());

        AppendEntriesRequest req = new AppendEntriesRequest(
            Term.of(1), NodeId.of("leader"),
            LogIndex.ZERO, Term.ZERO,
            List.of(), LogIndex.ZERO
        );
        AppendEntriesResponse resp = ch.send(
            MessageType.APPEND_ENTRIES_REQUEST,
            AppendEntriesCodec.encodeRequest(req),
            frame -> AppendEntriesCodec.decodeResponse(frame.body())
        ).get(3, TimeUnit.SECONDS);

        assertThat(resp.success()).isTrue();
    }

    @Test
    void appendEntriesWithEntriesRoundTrip() throws Exception {
        List<LogEntry> sentEntries = new ArrayList<>();

        dispatcherA.register(MessageType.APPEND_ENTRIES_REQUEST, body -> {
            AppendEntriesRequest req = AppendEntriesCodec.decodeRequest(body);
            sentEntries.addAll(req.entries());
            return AppendEntriesCodec.encodeResponse(
                new AppendEntriesResponse(req.term(), true,
                    req.entries().get(req.entries().size() - 1).index()));
        });

        RpcChannel ch = connectChannel(NodeId.of("a"), serverA.localPort());

        List<LogEntry> entries = List.of(
            new LogEntry(Term.of(1), LogIndex.of(1), new byte[]{10}),
            new LogEntry(Term.of(1), LogIndex.of(2), new byte[]{20, 30})
        );
        AppendEntriesRequest req = new AppendEntriesRequest(
            Term.of(1), NodeId.of("leader"),
            LogIndex.ZERO, Term.ZERO,
            entries, LogIndex.ZERO
        );
        AppendEntriesResponse resp = ch.send(
            MessageType.APPEND_ENTRIES_REQUEST,
            AppendEntriesCodec.encodeRequest(req),
            frame -> AppendEntriesCodec.decodeResponse(frame.body())
        ).get(3, TimeUnit.SECONDS);

        assertThat(resp.success()).isTrue();
        assertThat(resp.matchIndex()).isEqualTo(LogIndex.of(2));
        assertThat(sentEntries).hasSize(2);
    }

    // -----------------------------------------------------------------------
    // InstallSnapshot round-trip
    // -----------------------------------------------------------------------

    @Test
    void installSnapshotRoundTrip() throws Exception {
        dispatcherA.register(MessageType.INSTALL_SNAPSHOT_REQUEST, body -> {
            InstallSnapshotRequest req = InstallSnapshotCodec.decodeRequest(body);
            assertThat(req.done()).isTrue();
            return InstallSnapshotCodec.encodeResponse(
                new InstallSnapshotResponse(req.term()));
        });

        RpcChannel ch = connectChannel(NodeId.of("a"), serverA.localPort());

        byte[] snapshotData = new byte[512];
        for (int i = 0; i < snapshotData.length; i++) snapshotData[i] = (byte) (i % 128);

        InstallSnapshotRequest req = new InstallSnapshotRequest(
            Term.of(5), NodeId.of("leader"),
            LogIndex.of(100), Term.of(4),
            0, snapshotData, true
        );
        InstallSnapshotResponse resp = ch.send(
            MessageType.INSTALL_SNAPSHOT_REQUEST,
            InstallSnapshotCodec.encodeRequest(req),
            frame -> InstallSnapshotCodec.decodeResponse(frame.body())
        ).get(3, TimeUnit.SECONDS);

        assertThat(resp.term()).isEqualTo(Term.of(5));
    }

    // -----------------------------------------------------------------------
    // Partial-read simulation
    // -----------------------------------------------------------------------

    @Test
    void partialReadFrameIsReassembledCorrectly() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        Frame[] received = new Frame[1];

        NioServer partialServer = new NioServer("127.0.0.1", 0, (f, ctx) -> {
            received[0] = f;
            latch.countDown();
        });
        Thread t = new Thread(partialServer, "partial-srv");
        t.setDaemon(true);
        t.start();

        try {
            Frame frame = new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, new byte[]{1, 2, 3, 4, 5});
            ByteBuffer encoded = FrameEncoder.encode(frame);
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);

            try (SocketChannel client = SocketChannel.open(
                    new InetSocketAddress("127.0.0.1", partialServer.localPort()))) {
                // Send 1 byte at a time — exercises partial-read reassembly
                for (byte b : bytes) {
                    client.write(ByteBuffer.wrap(new byte[]{b}));
                    Thread.sleep(1);
                }
                assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
            }

            assertThat(received[0]).isEqualTo(frame);
        } finally {
            partialServer.close();
            t.interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Reconnection
    // -----------------------------------------------------------------------

    @Test
    void rpcChannelReconnectsAfterServerRestart() throws Exception {
        dispatcherA.register(MessageType.REQUEST_VOTE_REQUEST, body ->
            RequestVoteCodec.encodeResponse(new RequestVoteResponse(Term.of(1), true)));

        int portA = serverA.localPort();
        RpcChannel ch = connectChannel(NodeId.of("a"), portA);

        // First send succeeds
        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(1), NodeId.of("b"), LogIndex.ZERO, Term.ZERO);
        RequestVoteResponse r1 = ch.send(
            MessageType.REQUEST_VOTE_REQUEST,
            RequestVoteCodec.encodeRequest(req),
            frame -> RequestVoteCodec.decodeResponse(frame.body())
        ).get(3, TimeUnit.SECONDS);
        assertThat(r1.voteGranted()).isTrue();

        // Restart server A on the same port
        serverA.close();
        threadA.interrupt();

        // Wait a moment for the connection to be detected as closed
        Thread.sleep(200);

        NioServer serverA2 = new NioServer("127.0.0.1", portA, dispatcherA::dispatch);
        Thread threadA2 = new Thread(serverA2, "srv-a2");
        threadA2.setDaemon(true);
        threadA2.start();

        try {
            // Wait for reconnection (exponential backoff starts at 50ms)
            long deadline = System.currentTimeMillis() + 3000;
            while (!ch.isConnected() && System.currentTimeMillis() < deadline) {
                Thread.sleep(50);
            }
            assertThat(ch.isConnected())
                .as("channel should reconnect after server restart")
                .isTrue();
        } finally {
            serverA2.close();
            threadA2.interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private RpcChannel connectChannel(NodeId peerId, int targetPort) throws InterruptedException {
        RpcChannel ch = new RpcChannel(peerId, "127.0.0.1", targetPort, serverB, scheduler);
        ch.connect();
        long deadline = System.currentTimeMillis() + 3000;
        while (!ch.isConnected() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(ch.isConnected())
            .as("RpcChannel to port %d should be connected within 3s", targetPort)
            .isTrue();
        return ch;
    }
}
