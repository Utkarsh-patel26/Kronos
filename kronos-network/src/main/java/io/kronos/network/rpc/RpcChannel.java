package io.kronos.network.rpc;

import io.kronos.common.log.KronosLogger;
import io.kronos.common.model.NodeId;
import io.kronos.network.channel.ConnectionContext;
import io.kronos.network.channel.NioServer;
import io.kronos.network.protocol.Frame;
import io.kronos.network.protocol.FrameEncoder;
import io.kronos.network.protocol.MessageType;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Manages one persistent outbound TCP connection to a peer node.
 *
 * <p>Reconnects automatically with exponential backoff when the connection
 * drops. Concurrent sends return individual {@link CompletableFuture}s that
 * are resolved when the matching response frame arrives (keyed by reqId),
 * or failed after a 500 ms timeout.
 */
public final class RpcChannel {

    private static final KronosLogger log = KronosLogger.forClass(RpcChannel.class);

    private static final long RPC_TIMEOUT_MS = 500;
    private static final long[] BACKOFF_MS   = {50, 100, 200, 400, 800, 1600, 3200, 5000};

    private final NodeId                   peerId;
    private final String                   host;
    private final int                      port;
    private final NioServer                server;
    private final ScheduledExecutorService scheduler;

    private volatile ConnectionContext     ctx;
    private final ConcurrentHashMap<Integer, CompletableFuture<Frame>> pending;
    private final AtomicInteger            reqIdGen;
    private int                            backoffIdx;

    public RpcChannel(NodeId peerId, String host, int port,
                      NioServer server, ScheduledExecutorService scheduler) {
        this.peerId    = peerId;
        this.host      = host;
        this.port      = port;
        this.server    = server;
        this.scheduler = scheduler;
        this.pending   = new ConcurrentHashMap<>();
        this.reqIdGen  = new AtomicInteger(0);
    }

    /** Begin connecting. Must be called once before any sends. */
    public void connect() {
        tryConnect();
    }

    private void tryConnect() {
        try {
            SocketChannel ch = SocketChannel.open();
            ch.configureBlocking(false);
            ch.connect(new InetSocketAddress(host, port));

            ConnectionContext newCtx = new ConnectionContext(
                ch,
                (frame, c) -> onResponse(frame),
                this::onConnectionClosed);
            this.ctx = newCtx;
            server.registerOutbound(ch, newCtx);
            backoffIdx = 0;
            log.info("connecting to %s at %s:%d", peerId.value(), host, port);
        } catch (IOException e) {
            log.warn("failed to initiate connect to %s: %s", peerId.value(), e.getMessage());
            scheduleReconnect();
        }
    }

    private void onConnectionClosed() {
        ctx = null;
        failPending(new IOException("connection to " + peerId.value() + " closed"));
        scheduleReconnect();
    }

    private void scheduleReconnect() {
        long delay = BACKOFF_MS[Math.min(backoffIdx++, BACKOFF_MS.length - 1)];
        log.info("reconnecting to %s in %d ms", peerId.value(), delay);
        try {
            scheduler.schedule(this::tryConnect, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ignored) {
            // Scheduler has been shut down — the node is stopping, no reconnect needed.
        }
    }

    /**
     * Send a request frame and return a future for the decoded response.
     *
     * @param type    message type (must be a request type)
     * @param body    serialized request body
     * @param decoder function to decode the raw response frame into {@code T}
     */
    public <T> CompletableFuture<T> send(MessageType type, byte[] body, Function<Frame, T> decoder) {
        ConnectionContext current = ctx;
        if (current == null || !current.channel().isConnected()) {
            return CompletableFuture.failedFuture(
                new IOException("not connected to " + peerId.value()));
        }

        int reqId = reqIdGen.incrementAndGet();
        CompletableFuture<Frame> raw = new CompletableFuture<>();
        pending.put(reqId, raw);

        Frame frame = new Frame(type, reqId, body);
        ByteBuffer buf = FrameEncoder.encode(frame);
        server.enqueueWrite(current, buf);

        return raw
            .orTimeout(RPC_TIMEOUT_MS, TimeUnit.MILLISECONDS)
            .thenApply(decoder)
            .whenComplete((r, ex) -> pending.remove(reqId));
    }

    /** Called by the NIO thread when a response frame arrives on this channel. */
    void onResponse(Frame frame) {
        CompletableFuture<Frame> f = pending.get(frame.reqId());
        if (f != null) {
            f.complete(frame);
        } else {
            log.warn("no pending future for reqId=%d from %s", frame.reqId(), peerId.value());
        }
    }

    public void disconnect() {
        ConnectionContext current = ctx;
        ctx = null;
        if (current != null) {
            try { current.channel().close(); } catch (IOException ignored) {}
        }
        failPending(new IOException("channel disconnected"));
    }

    public boolean isConnected() {
        ConnectionContext current = ctx;
        return current != null && current.channel().isConnected();
    }

    public NodeId peerId() { return peerId; }

    private void failPending(IOException reason) {
        pending.values().forEach(f -> f.completeExceptionally(reason));
        pending.clear();
    }
}
