package io.kronos.network.channel;

import io.kronos.network.protocol.Frame;
import io.kronos.network.protocol.FrameEncoder;
import io.kronos.network.protocol.MessageType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

class NioServerTest {

    private NioServer server;
    private Thread    serverThread;
    private int       port;

    @BeforeEach
    void startServer() throws IOException {
        // Port 0 lets the OS assign a free port
        server = new NioServer("127.0.0.1", 0, (frame, ctx) -> {});
        port = server.localPort();
        serverThread = new Thread(server, "nio-server-test");
        serverThread.setDaemon(true);
        serverThread.start();
    }

    @AfterEach
    void stopServer() throws IOException {
        server.close();
        serverThread.interrupt();
    }

    @Test
    void acceptsConnectionFromClient() throws Exception {
        try (SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
            assertThat(ch.isConnected()).isTrue();
        }
    }

    @Test
    void receivesFrameFromClient() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Frame> received = new AtomicReference<>();
        server.close();

        server = new NioServer("127.0.0.1", 0, (frame, ctx) -> {
            received.set(frame);
            latch.countDown();
        });
        port = server.localPort();
        serverThread = new Thread(server, "nio-server-recv-test");
        serverThread.setDaemon(true);
        serverThread.start();

        Frame sent = new Frame(MessageType.REQUEST_VOTE_REQUEST, 42, new byte[]{1, 2, 3});
        try (SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
            ByteBuffer buf = FrameEncoder.encode(sent);
            while (buf.hasRemaining()) ch.write(buf);
            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(received.get()).isEqualTo(sent);
    }

    @Test
    void receivesMultipleFramesFromOneClient() throws Exception {
        CountDownLatch latch = new CountDownLatch(3);
        List<Frame> received = new ArrayList<>();
        server.close();

        server = new NioServer("127.0.0.1", 0, (frame, ctx) -> {
            received.add(frame);
            latch.countDown();
        });
        port = server.localPort();
        serverThread = new Thread(server, "nio-multi-test");
        serverThread.setDaemon(true);
        serverThread.start();

        Frame f1 = new Frame(MessageType.REQUEST_VOTE_REQUEST,    1, new byte[]{1});
        Frame f2 = new Frame(MessageType.APPEND_ENTRIES_REQUEST,  2, new byte[]{2, 3});
        Frame f3 = new Frame(MessageType.INSTALL_SNAPSHOT_REQUEST, 3, new byte[]{4, 5, 6});

        try (SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
            for (Frame f : List.of(f1, f2, f3)) {
                ByteBuffer buf = FrameEncoder.encode(f);
                while (buf.hasRemaining()) ch.write(buf);
            }
            assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(received).containsExactly(f1, f2, f3);
    }

    @Test
    void sendsFrameBackToClient() throws Exception {
        Frame toSend = new Frame(MessageType.REQUEST_VOTE_RESPONSE, 7, new byte[]{99});
        server.close();

        server = new NioServer("127.0.0.1", 0, (frame, ctx) ->
            server.enqueueWrite(ctx, FrameEncoder.encode(toSend)));
        port = server.localPort();
        serverThread = new Thread(server, "nio-write-test");
        serverThread.setDaemon(true);
        serverThread.start();

        Frame trigger = new Frame(MessageType.REQUEST_VOTE_REQUEST, 7, new byte[0]);
        ByteBuffer readBuf = ByteBuffer.allocate(256);

        try (SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port))) {
            ch.write(FrameEncoder.encode(trigger));

            // Read until we have a full frame
            long deadline = System.currentTimeMillis() + 2000;
            while (readBuf.position() < FrameEncoder.HEADER_SIZE + 1
                   && System.currentTimeMillis() < deadline) {
                ch.read(readBuf);
            }
            readBuf.flip();

            // Verify magic and reqId in the response
            short magic = readBuf.getShort();
            assertThat(magic).isEqualTo(FrameEncoder.MAGIC);
            short type = readBuf.getShort();
            assertThat(type).isEqualTo(MessageType.REQUEST_VOTE_RESPONSE.code);
            int reqId = readBuf.getInt();
            assertThat(reqId).isEqualTo(7);
        }
    }

    @Test
    void handlesMultipleConcurrentConnections() throws Exception {
        int clientCount = 5;
        CountDownLatch latch = new CountDownLatch(clientCount);
        server.close();

        server = new NioServer("127.0.0.1", 0, (frame, ctx) -> latch.countDown());
        port = server.localPort();
        serverThread = new Thread(server, "nio-multi-conn-test");
        serverThread.setDaemon(true);
        serverThread.start();

        List<SocketChannel> channels = new ArrayList<>();
        try {
            for (int i = 0; i < clientCount; i++) {
                SocketChannel ch = SocketChannel.open(new InetSocketAddress("127.0.0.1", port));
                channels.add(ch);
                Frame f = new Frame(MessageType.REQUEST_VOTE_REQUEST, i, new byte[]{(byte) i});
                ByteBuffer buf = FrameEncoder.encode(f);
                while (buf.hasRemaining()) ch.write(buf);
            }
            assertThat(latch.await(3, TimeUnit.SECONDS)).isTrue();
        } finally {
            for (SocketChannel ch : channels) ch.close();
        }
    }

    @Test
    void localPortReturnsActualBoundPort() {
        assertThat(port).isGreaterThan(0);
        assertThat(port).isLessThan(65536);
    }
}
