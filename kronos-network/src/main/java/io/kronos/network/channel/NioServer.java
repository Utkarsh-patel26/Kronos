package io.kronos.network.channel;

import io.kronos.common.log.KronosLogger;
import io.kronos.network.protocol.Frame;
import io.kronos.network.protocol.FrameDecoder;
import io.kronos.network.protocol.ProtocolException;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;

/**
 * Single-threaded NIO selector loop.
 *
 * <p>Handles both inbound connections (via its embedded ServerSocketChannel) and
 * outbound connections registered by {@link io.kronos.network.rpc.RpcChannel}
 * through {@link #registerOutbound}. All channel operations happen on the
 * selector thread; external threads interact only via the pending-tasks queue
 * and {@link #enqueueWrite}.
 */
public final class NioServer implements Runnable, Closeable {

    private static final KronosLogger log = KronosLogger.forClass(NioServer.class);
    private static final int SELECT_TIMEOUT_MS = 100;

    private final Selector                            selector;
    private final ServerSocketChannel                 serverChannel;
    private final BiConsumer<Frame, ConnectionContext> inboundHandler;
    private final Queue<Runnable>                     pendingTasks;
    private volatile boolean                          running;

    public NioServer(String host, int port,
                     BiConsumer<Frame, ConnectionContext> inboundHandler) throws IOException {
        this.inboundHandler = inboundHandler;
        this.pendingTasks   = new ConcurrentLinkedQueue<>();
        this.running        = true;
        this.selector       = Selector.open();

        serverChannel = ServerSocketChannel.open();
        serverChannel.configureBlocking(false);
        serverChannel.bind(new InetSocketAddress(host, port));
        serverChannel.register(selector, SelectionKey.OP_ACCEPT);
        log.info("bound to %s:%d", host, port);
    }

    @Override
    public void run() {
        while (running && !Thread.currentThread().isInterrupted()) {
            try {
                selector.select(SELECT_TIMEOUT_MS);
                drainTasks();
                processSelectedKeys();
            } catch (ClosedSelectorException e) {
                break;
            } catch (IOException e) {
                if (running) log.error("selector error", e);
            }
        }
        log.info("stopped");
    }

    private void processSelectedKeys() {
        try {
            Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext()) {
                SelectionKey key = it.next();
                it.remove();
                if (key.isValid()) processKey(key);
            }
        } catch (ConcurrentModificationException ignored) {
            // close() called selectedKeys.clear() via selector.close() from another thread
            // while we were iterating — shutdown is in progress, stop processing keys.
        }
    }

    private void processKey(SelectionKey key) {
        try {
            if      (key.isAcceptable())               accept(key);
            else if (key.isConnectable())              connect(key);
            else {
                if (key.isReadable())                  read(key);
                if (key.isValid() && key.isWritable()) write(key);
            }
        } catch (Exception e) {
            log.warn("key handler error: %s", e.getMessage());
            closeKey(key);
        }
    }

    // -----------------------------------------------------------------------
    // Key handlers — all run on the selector thread
    // -----------------------------------------------------------------------

    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel srv = (ServerSocketChannel) key.channel();
        SocketChannel ch = srv.accept();
        if (ch == null) return;
        ch.configureBlocking(false);
        ConnectionContext ctx = new ConnectionContext(ch, inboundHandler, null);
        ch.register(selector, SelectionKey.OP_READ, ctx);
        log.debug("accepted %s", remoteAddr(ch));
    }

    private void connect(SelectionKey key) {
        ConnectionContext ctx = (ConnectionContext) key.attachment();
        try {
            if (ctx.channel.finishConnect()) {
                key.interestOps(SelectionKey.OP_READ);
                log.debug("connected to %s", remoteAddr(ctx.channel));
            }
        } catch (IOException e) {
            log.warn("connect failed: %s", e.getMessage());
            closeKey(key);
        }
    }

    private void read(SelectionKey key) throws IOException {
        ConnectionContext ctx = (ConnectionContext) key.attachment();
        ByteBuffer buf = ctx.readBuf;
        int n = ctx.channel.read(buf);
        if (n == -1) {
            log.debug("peer closed: %s", remoteAddr(ctx.channel));
            closeKey(key);
            return;
        }
        if (n > 0) decodeAndDispatch(key, ctx, buf);
    }

    private void decodeAndDispatch(SelectionKey key, ConnectionContext ctx, ByteBuffer buf) {
        buf.flip();
        try {
            List<Frame> frames = FrameDecoder.decode(buf);
            for (Frame f : frames) {
                ctx.frameHandler.accept(f, ctx);
            }
        } catch (ProtocolException e) {
            log.warn("protocol error from %s: %s", remoteAddr(ctx.channel), e.getMessage());
            closeKey(key);
            return;
        }
        buf.compact();
    }

    private void write(SelectionKey key) throws IOException {
        ConnectionContext ctx = (ConnectionContext) key.attachment();
        ByteBuffer buf;
        while ((buf = ctx.writeQueue.peek()) != null) {
            ctx.channel.write(buf);
            if (buf.hasRemaining()) break; // channel buffer full, wait for next OP_WRITE
            ctx.writeQueue.poll();
        }
        if (ctx.writeQueue.isEmpty()) {
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);
        }
    }

    // -----------------------------------------------------------------------
    // Cross-thread API
    // -----------------------------------------------------------------------

    /**
     * Register an outbound SocketChannel with the selector loop.
     * Safe to call from any thread.
     */
    public void registerOutbound(SocketChannel ch, ConnectionContext ctx) {
        submit(() -> {
            try {
                ch.register(selector, SelectionKey.OP_CONNECT, ctx);
            } catch (ClosedChannelException e) {
                log.warn("outbound channel closed before registration");
                if (ctx.onClose != null) ctx.onClose.run();
            }
        });
    }

    /**
     * Enqueue a write on an existing connection.
     * Safe to call from any thread.
     */
    public void enqueueWrite(ConnectionContext ctx, ByteBuffer buf) {
        ctx.enqueue(buf);
        submit(() -> {
            SelectionKey key = ctx.channel.keyFor(selector);
            if (key != null && key.isValid()) {
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            }
        });
    }

    public int localPort() {
        try {
            return ((InetSocketAddress) serverChannel.getLocalAddress()).getPort();
        } catch (IOException e) {
            throw new IllegalStateException("failed to get local port", e);
        }
    }

    public void stop() {
        running = false;
        selector.wakeup();
    }

    @Override
    public void close() throws IOException {
        running = false;
        if (!selector.isOpen()) return;
        selector.wakeup();
        for (SelectionKey key : selector.keys()) {
            try {
                key.channel().close();
            } catch (IOException ignored) {
                // best-effort close on shutdown
            }
        }
        selector.close();
    }

    // -----------------------------------------------------------------------
    // Internals
    // -----------------------------------------------------------------------

    private void submit(Runnable task) {
        pendingTasks.add(task);
        selector.wakeup();
    }

    private void drainTasks() {
        Runnable t;
        while ((t = pendingTasks.poll()) != null) {
            try { t.run(); } catch (Exception e) {
                log.warn("pending task error: %s", e.getMessage());
            }
        }
    }

    private void closeKey(SelectionKey key) {
        ConnectionContext ctx = key.attachment() instanceof ConnectionContext c ? c : null;
        key.cancel();
        try {
            key.channel().close();
        } catch (IOException ignored) {
            // channel may already be closed
        }
        if (ctx != null && ctx.onClose != null) {
            try { ctx.onClose.run(); } catch (Exception e) {
                log.warn("onClose callback error: %s", e.getMessage());
            }
        }
    }

    private static String remoteAddr(SocketChannel ch) {
        try {
            SocketAddress addr = ch.getRemoteAddress();
            return addr != null ? addr.toString() : "unknown";
        } catch (IOException e) {
            return "unknown";
        }
    }
}
