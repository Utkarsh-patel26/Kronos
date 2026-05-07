package io.kronos.network.rpc;

import io.kronos.network.channel.ConnectionContext;
import io.kronos.network.protocol.Frame;
import io.kronos.network.protocol.MessageType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

class RpcDispatcherTest {

    private RpcDispatcher dispatcher;
    private List<ByteBuffer> written;
    private ConnectionContext ctx;

    @BeforeEach
    void setUp() {
        // Direct executor runs tasks inline on the calling thread
        dispatcher = new RpcDispatcher(Runnable::run);
        written = new ArrayList<>();
        dispatcher.setWriter((c, buf) -> written.add(buf));

        SocketChannel mockCh = mock(SocketChannel.class);
        ctx = new ConnectionContext(mockCh, (f, c) -> {}, null);
    }

    @Test
    void dispatchesRequestToRegisteredHandler() {
        byte[] responseBody = {9, 8, 7};
        dispatcher.register(MessageType.REQUEST_VOTE_REQUEST, body -> responseBody);

        Frame request = new Frame(MessageType.REQUEST_VOTE_REQUEST, 5, new byte[]{1, 2});
        dispatcher.dispatch(request, ctx);

        assertThat(written).hasSize(1);
    }

    @Test
    void responseHasCorrectTypeAndReqId() {
        dispatcher.register(MessageType.REQUEST_VOTE_REQUEST, body -> new byte[]{42});

        Frame request = new Frame(MessageType.REQUEST_VOTE_REQUEST, 99, new byte[]{});
        dispatcher.dispatch(request, ctx);

        assertThat(written).hasSize(1);
        ByteBuffer resp = written.get(0);
        // Skip magic (2) + check type code at offset 2
        short typeCode = resp.getShort(2);
        assertThat(typeCode).isEqualTo(MessageType.REQUEST_VOTE_RESPONSE.code);
        // ReqId at offset 4 should match request's reqId
        int reqId = resp.getInt(4);
        assertThat(reqId).isEqualTo(99);
    }

    @Test
    void dispatchIgnoresUnknownMessageType() {
        // No handler for APPEND_ENTRIES_REQUEST — should not throw, not write anything
        Frame request = new Frame(MessageType.APPEND_ENTRIES_REQUEST, 1, new byte[0]);

        assertThatCode(() -> dispatcher.dispatch(request, ctx)).doesNotThrowAnyException();
        assertThat(written).isEmpty();
    }

    @Test
    void rejectRegisteringHandlerForResponseType() {
        assertThatThrownBy(() ->
            dispatcher.register(MessageType.REQUEST_VOTE_RESPONSE, body -> body))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void dispatchRoutesEachTypeToItsOwnHandler() {
        List<String> calls = new ArrayList<>();
        dispatcher.register(MessageType.REQUEST_VOTE_REQUEST,
            body -> { calls.add("vote"); return new byte[0]; });
        dispatcher.register(MessageType.APPEND_ENTRIES_REQUEST,
            body -> { calls.add("append"); return new byte[0]; });

        dispatcher.dispatch(new Frame(MessageType.REQUEST_VOTE_REQUEST,    1, new byte[0]), ctx);
        dispatcher.dispatch(new Frame(MessageType.APPEND_ENTRIES_REQUEST,  2, new byte[0]), ctx);
        dispatcher.dispatch(new Frame(MessageType.REQUEST_VOTE_REQUEST,    3, new byte[0]), ctx);

        assertThat(calls).containsExactly("vote", "append", "vote");
    }

    @Test
    void handlerReceivesCorrectBody() {
        byte[] expectedBody = {1, 2, 3, 4};
        byte[][] capturedBody = new byte[1][];
        dispatcher.register(MessageType.INSTALL_SNAPSHOT_REQUEST, body -> {
            capturedBody[0] = body;
            return new byte[0];
        });

        dispatcher.dispatch(new Frame(MessageType.INSTALL_SNAPSHOT_REQUEST, 1, expectedBody), ctx);

        assertThat(capturedBody[0]).isEqualTo(expectedBody);
    }

    @Test
    void dispatchIsAsyncWithRealExecutor() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        RpcDispatcher asyncDispatcher = new RpcDispatcher(r -> new Thread(r).start());
        asyncDispatcher.setWriter((c, buf) -> written.add(buf));
        asyncDispatcher.register(MessageType.REQUEST_VOTE_REQUEST, body -> {
            latch.countDown();
            return new byte[0];
        });

        asyncDispatcher.dispatch(new Frame(MessageType.REQUEST_VOTE_REQUEST, 1, new byte[0]), ctx);

        assertThat(latch.await(2, TimeUnit.SECONDS)).isTrue();
    }
}
