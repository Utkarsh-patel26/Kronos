package io.kronos.network.rpc;

import io.kronos.common.log.KronosLogger;
import io.kronos.network.channel.ConnectionContext;
import io.kronos.network.protocol.Frame;
import io.kronos.network.protocol.FrameEncoder;
import io.kronos.network.protocol.MessageType;

import java.nio.ByteBuffer;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.BiConsumer;

/**
 * Routes incoming request frames to registered handlers and writes responses
 * back through the connection.
 *
 * <p>Usage:
 * <pre>{@code
 *   RpcDispatcher dispatcher = new RpcDispatcher(executor);
 *   NioServer server = new NioServer(host, port, dispatcher::dispatch);
 *   dispatcher.setWriter(server::enqueueWrite);
 *   dispatcher.register(MessageType.REQUEST_VOTE_REQUEST, body -> { ... });
 * }</pre>
 */
public final class RpcDispatcher {

    private static final KronosLogger log = KronosLogger.forClass(RpcDispatcher.class);

    private final Map<MessageType, RpcHandler>       handlers;
    private final Executor                           executor;
    private       BiConsumer<ConnectionContext, ByteBuffer> writer;

    public RpcDispatcher(Executor executor) {
        this.handlers = new EnumMap<>(MessageType.class);
        this.executor = executor;
    }

    /** Wire up the write-back path. Must be called before any frames arrive. */
    public void setWriter(BiConsumer<ConnectionContext, ByteBuffer> writer) {
        this.writer = writer;
    }

    public void register(MessageType type, RpcHandler handler) {
        if (!type.isRequest()) {
            throw new IllegalArgumentException("can only register handlers for request types, got: " + type);
        }
        handlers.put(type, handler);
    }

    /**
     * Dispatch one frame. Called by the NIO thread; hands work off to the
     * executor so Raft logic never blocks the selector loop.
     */
    public void dispatch(Frame frame, ConnectionContext ctx) {
        RpcHandler handler = handlers.get(frame.type());
        if (handler == null) {
            log.warn("no handler registered for %s", frame.type());
            return;
        }
        try {
            executor.execute(() -> {
                byte[] responseBody = handler.handle(frame.body());
                Frame response = new Frame(frame.type().responseType(), frame.reqId(), responseBody);
                ByteBuffer encoded = FrameEncoder.encode(response);
                writer.accept(ctx, encoded);
            });
        } catch (RejectedExecutionException ignored) {
            // Executor shut down — node is stopping, drop the frame silently.
        }
    }
}
