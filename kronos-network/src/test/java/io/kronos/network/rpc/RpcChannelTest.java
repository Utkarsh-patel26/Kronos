package io.kronos.network.rpc;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.channel.NioServer;
import io.kronos.network.codec.RequestVoteCodec;
import io.kronos.network.protocol.MessageType;
import io.kronos.network.rpc.message.RequestVoteRequest;
import io.kronos.network.rpc.message.RequestVoteResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for RpcChannel — requires real NioServer loops running in threads.
 */
class RpcChannelTest {

    private NioServer serverA;   // the "peer" we send RPCs to
    private NioServer serverB;   // the "local" server that owns the RpcChannel
    private RpcDispatcher dispatcherA;
    private ScheduledExecutorService scheduler;
    private ExecutorService executor;
    private Thread threadA;
    private Thread threadB;

    @BeforeEach
    void setUp() throws IOException {
        executor  = Executors.newCachedThreadPool();
        scheduler = Executors.newSingleThreadScheduledExecutor();

        dispatcherA = new RpcDispatcher(executor);
        serverA = new NioServer("127.0.0.1", 0, dispatcherA::dispatch);
        dispatcherA.setWriter(serverA::enqueueWrite);

        serverB = new NioServer("127.0.0.1", 0, (f, ctx) -> {});

        threadA = new Thread(serverA, "server-a");
        threadB = new Thread(serverB, "server-b");
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
        scheduler.shutdownNow();
        executor.shutdownNow();
    }

    @Test
    void sendsRequestAndReceivesResponse() throws Exception {
        dispatcherA.register(MessageType.REQUEST_VOTE_REQUEST, body -> {
            RequestVoteRequest req = RequestVoteCodec.decodeRequest(body);
            return RequestVoteCodec.encodeResponse(new RequestVoteResponse(req.term(), true));
        });

        RpcChannel channel = new RpcChannel(
            NodeId.of("node-a"), "127.0.0.1", serverA.localPort(), serverB, scheduler);
        channel.connect();
        awaitConnected(channel);

        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(2), NodeId.of("node-b"), LogIndex.of(3), Term.of(1));
        byte[] reqBody = RequestVoteCodec.encodeRequest(req);

        CompletableFuture<RequestVoteResponse> future = channel.send(
            MessageType.REQUEST_VOTE_REQUEST, reqBody,
            frame -> RequestVoteCodec.decodeResponse(frame.body()));

        RequestVoteResponse resp = future.get(3, TimeUnit.SECONDS);
        assertThat(resp.voteGranted()).isTrue();
        assertThat(resp.term()).isEqualTo(Term.of(2));
    }

    @Test
    void concurrentSendsAllResolve() throws Exception {
        dispatcherA.register(MessageType.REQUEST_VOTE_REQUEST, body ->
            RequestVoteCodec.encodeResponse(new RequestVoteResponse(Term.of(1), true)));

        RpcChannel channel = new RpcChannel(
            NodeId.of("node-a"), "127.0.0.1", serverA.localPort(), serverB, scheduler);
        channel.connect();
        awaitConnected(channel);

        int count = 20;
        RequestVoteRequest req = new RequestVoteRequest(
            Term.of(1), NodeId.of("node-b"), LogIndex.ZERO, Term.ZERO);
        byte[] reqBody = RequestVoteCodec.encodeRequest(req);

        List<CompletableFuture<RequestVoteResponse>> futures = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            futures.add(channel.send(
                MessageType.REQUEST_VOTE_REQUEST, reqBody,
                frame -> RequestVoteCodec.decodeResponse(frame.body())));
        }

        List<RequestVoteResponse> responses = new ArrayList<>();
        for (CompletableFuture<RequestVoteResponse> f : futures) {
            responses.add(f.get(5, TimeUnit.SECONDS));
        }
        assertThat(responses).hasSize(count);
        responses.forEach(r -> assertThat(r.voteGranted()).isTrue());
    }

    @Test
    void failsImmediatelyWhenNotConnected() {
        RpcChannel channel = new RpcChannel(
            NodeId.of("node-a"), "127.0.0.1", serverA.localPort(), serverB, scheduler);
        // No connect() call

        CompletableFuture<RequestVoteResponse> future = channel.send(
            MessageType.REQUEST_VOTE_REQUEST, new byte[0],
            frame -> RequestVoteCodec.decodeResponse(frame.body()));

        assertThat(future).isCompletedExceptionally();
    }

    @Test
    void isConnectedAfterConnecting() throws Exception {
        RpcChannel channel = new RpcChannel(
            NodeId.of("node-a"), "127.0.0.1", serverA.localPort(), serverB, scheduler);
        channel.connect();
        awaitConnected(channel);
        assertThat(channel.isConnected()).isTrue();
    }

    @Test
    void disconnectFailsPendingFutures() throws Exception {
        dispatcherA.register(MessageType.REQUEST_VOTE_REQUEST, body -> {
            try { Thread.sleep(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return RequestVoteCodec.encodeResponse(new RequestVoteResponse(Term.of(1), false));
        });

        RpcChannel channel = new RpcChannel(
            NodeId.of("node-a"), "127.0.0.1", serverA.localPort(), serverB, scheduler);
        channel.connect();
        awaitConnected(channel);

        CompletableFuture<RequestVoteResponse> future = channel.send(
            MessageType.REQUEST_VOTE_REQUEST, new byte[0],
            frame -> RequestVoteCodec.decodeResponse(frame.body()));

        channel.disconnect();

        assertThatThrownBy(() -> future.get(1, TimeUnit.SECONDS))
            .hasCauseInstanceOf(IOException.class);
    }

    private static void awaitConnected(RpcChannel channel) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 3000;
        while (!channel.isConnected() && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(channel.isConnected())
            .as("RpcChannel should be connected within 3 seconds")
            .isTrue();
    }
}
