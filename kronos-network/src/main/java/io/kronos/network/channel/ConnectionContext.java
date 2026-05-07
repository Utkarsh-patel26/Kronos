package io.kronos.network.channel;

import io.kronos.network.protocol.Frame;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.BiConsumer;

public final class ConnectionContext {

    static final int READ_BUF_SIZE = 65536; // 64 KiB

    final SocketChannel                        channel;
    final ByteBuffer                           readBuf;
    final Queue<ByteBuffer>                    writeQueue;
    final BiConsumer<Frame, ConnectionContext>  frameHandler;
    final Runnable                             onClose;  // null for inbound connections

    public ConnectionContext(SocketChannel channel,
                             BiConsumer<Frame, ConnectionContext> frameHandler,
                             Runnable onClose) {
        this.channel      = channel;
        this.readBuf      = ByteBuffer.allocate(READ_BUF_SIZE);
        this.writeQueue   = new ConcurrentLinkedQueue<>();
        this.frameHandler = frameHandler;
        this.onClose      = onClose;
    }

    public SocketChannel channel() { return channel; }

    /** Enqueue a buffer to be written; the NIO thread will drain the queue on OP_WRITE. */
    public void enqueue(ByteBuffer buf) {
        writeQueue.add(buf);
    }
}
