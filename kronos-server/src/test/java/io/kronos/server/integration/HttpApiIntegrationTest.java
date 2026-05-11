package io.kronos.server.integration;

import io.kronos.common.model.NodeId;
import io.kronos.network.channel.NioServer;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.raft.core.RaftNode;
import io.kronos.raft.core.RaftRole;
import io.kronos.raft.log.InMemoryRaftLog;
import io.kronos.server.http.HttpApiServer;
import io.kronos.server.kv.KvStateMachine;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end HTTP API tests.  Each test spins up a real single-node Kronos
 * cluster (single node wins election immediately), attaches the HTTP API
 * server, and issues real HTTP requests via {@link HttpURLConnection}.
 */
class HttpApiIntegrationTest {

    @TempDir
    Path tmpDir;

    private NioServer                server;
    private RaftNode                 raftNode;
    private KvStateMachine           sm;
    private HttpApiServer            httpApi;
    private ScheduledExecutorService raftThread;
    private int                      httpPort;

    @BeforeEach
    void setUp() throws Exception {
        NodeId myId = NodeId.of("n1");

        raftThread = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-n1");
            t.setDaemon(true);
            return t;
        });

        RpcDispatcher dispatcher = new RpcDispatcher(raftThread);
        server = new NioServer("127.0.0.1", 0, dispatcher::dispatch);
        dispatcher.setWriter(server::enqueueWrite);

        Thread nioThread = new Thread(server, "nio-n1");
        nioThread.setDaemon(true);

        sm = new KvStateMachine();
        InMemoryRaftLog log = new InMemoryRaftLog();

        // Single-node cluster — no peers, wins election immediately
        raftNode = new RaftNode(myId, List.of(), Map.of(), dispatcher,
            log, tmpDir,
            150, 300, 50, raftThread, sm);

        // Register handlers before NIO starts (correct order)
        raftNode.start();
        nioThread.start();

        // Wait for leadership (single-node cluster elects itself immediately)
        long deadline = System.currentTimeMillis() + 3000;
        while (raftNode.role() != RaftRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(raftNode.role())
            .as("single-node cluster must become leader").isEqualTo(RaftRole.LEADER);

        // Start HTTP API on a random port
        httpPort = findFreePort();
        httpApi = new HttpApiServer(raftNode, sm, List.of(),
            Map.of(), httpPort);
        httpApi.start();
    }

    @AfterEach
    void tearDown() {
        try { httpApi.stop();    } catch (Exception ignored) {}
        try { raftNode.stop();   } catch (Exception ignored) {}
        try { server.close();    } catch (Exception ignored) {}
        raftThread.shutdownNow();
    }

    // -----------------------------------------------------------------------
    // PUT / GET
    // -----------------------------------------------------------------------

    @Test
    void putAndGetRoundTrip() throws Exception {
        HttpResponse put = request("PUT", "/keys/hello", "world".getBytes());
        assertThat(put.status).isEqualTo(200);
        assertThat(put.body()).contains("committed");

        HttpResponse get = request("GET", "/keys/hello", null);
        assertThat(get.status).isEqualTo(200);
        assertThat(new String(get.rawBody, StandardCharsets.UTF_8)).isEqualTo("world");
    }

    @Test
    void getReturns404ForMissingKey() throws Exception {
        HttpResponse get = request("GET", "/keys/ghost", null);
        assertThat(get.status).isEqualTo(404);
    }

    @Test
    void putOverwritesExistingKey() throws Exception {
        request("PUT", "/keys/k", "v1".getBytes());
        request("PUT", "/keys/k", "v2".getBytes());

        HttpResponse get = request("GET", "/keys/k", null);
        assertThat(new String(get.rawBody, StandardCharsets.UTF_8)).isEqualTo("v2");
    }

    // -----------------------------------------------------------------------
    // DELETE
    // -----------------------------------------------------------------------

    @Test
    void deleteRemovesKey() throws Exception {
        request("PUT", "/keys/d", "val".getBytes());
        HttpResponse del = request("DELETE", "/keys/d", null);
        assertThat(del.status).isEqualTo(200);

        HttpResponse get = request("GET", "/keys/d", null);
        assertThat(get.status).isEqualTo(404);
    }

    @Test
    void deleteOfMissingKeyIsIdempotent() throws Exception {
        HttpResponse del = request("DELETE", "/keys/nope", null);
        assertThat(del.status).isEqualTo(200);
        assertThat(del.body()).contains("committed");
    }

    // -----------------------------------------------------------------------
    // CAS
    // -----------------------------------------------------------------------

    @Test
    void casSwapsValueWhenExpectedMatches() throws Exception {
        request("PUT", "/keys/x", "v1".getBytes());

        String casBody = casJson("v1", "v2");
        HttpResponse cas = requestJson("POST", "/keys/x/cas", casBody);
        assertThat(cas.status).isEqualTo(200);

        HttpResponse get = request("GET", "/keys/x", null);
        assertThat(new String(get.rawBody, StandardCharsets.UTF_8)).isEqualTo("v2");
    }

    @Test
    void casKeepsOldValueWhenExpectedMismatches() throws Exception {
        request("PUT", "/keys/x", "v1".getBytes());

        String casBody = casJson("wrong", "v2");
        HttpResponse cas = requestJson("POST", "/keys/x/cas", casBody);
        assertThat(cas.status).isEqualTo(200);

        HttpResponse get = request("GET", "/keys/x", null);
        assertThat(new String(get.rawBody, StandardCharsets.UTF_8)).isEqualTo("v1");
    }

    @Test
    void casRejectsMalformedBody() throws Exception {
        HttpResponse cas = requestJson("POST", "/keys/x/cas", "{bad json}");
        assertThat(cas.status).isEqualTo(400);
    }

    // -----------------------------------------------------------------------
    // Method validation
    // -----------------------------------------------------------------------

    @Test
    void unsupportedMethodReturns405() throws Exception {
        // HttpURLConnection rejects non-standard methods; use java.net.http.HttpClient
        java.net.http.HttpClient client = java.net.http.HttpClient.newHttpClient();
        java.net.http.HttpRequest req = java.net.http.HttpRequest.newBuilder()
            .uri(URI.create("http://127.0.0.1:" + httpPort + "/keys/k"))
            .method("PATCH", java.net.http.HttpRequest.BodyPublishers.noBody())
            .build();
        java.net.http.HttpResponse<String> resp =
            client.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
        assertThat(resp.statusCode()).isEqualTo(405);
    }

    // -----------------------------------------------------------------------
    // Binary values
    // -----------------------------------------------------------------------

    @Test
    void putAndGetBinaryValue() throws Exception {
        byte[] binary = {0x00, 0x01, (byte) 0xFF, (byte) 0xFE};
        request("PUT", "/keys/bin", binary);

        HttpResponse get = request("GET", "/keys/bin", null);
        assertThat(get.status).isEqualTo(200);
        assertThat(get.rawBody).isEqualTo(binary);
    }

    // -----------------------------------------------------------------------
    // Cluster info
    // -----------------------------------------------------------------------

    @Test
    void clusterEndpointReturnsJson() throws Exception {
        HttpResponse resp = request("GET", "/cluster", null);
        assertThat(resp.status).isEqualTo(200);
        assertThat(resp.body()).contains("\"nodeId\"");
        assertThat(resp.body()).contains("\"role\"");
        assertThat(resp.body()).contains("LEADER");
        assertThat(resp.body()).contains("\"commitIndex\"");
    }

    @Test
    void clusterLogEndpointReturnsEntriesAfterWrites() throws Exception {
        request("PUT", "/keys/a", "1".getBytes());
        request("PUT", "/keys/b", "2".getBytes());

        // Brief pause to ensure apply has run
        Thread.sleep(100);

        HttpResponse resp = request("GET", "/cluster/log", null);
        assertThat(resp.status).isEqualTo(200);
        assertThat(resp.body()).contains("\"entries\"");
        assertThat(resp.body()).contains("\"index\"");
    }

    // -----------------------------------------------------------------------
    // Metrics
    // -----------------------------------------------------------------------

    @Test
    void metricsEndpointReturnsPrometheusFormat() throws Exception {
        HttpResponse resp = request("GET", "/metrics", null);
        assertThat(resp.status).isEqualTo(200);
        assertThat(resp.body()).contains("kronos_current_term");
        assertThat(resp.body()).contains("kronos_is_leader");
        assertThat(resp.body()).contains("# TYPE");
        assertThat(resp.body()).contains("# HELP");
    }

    // -----------------------------------------------------------------------
    // Latency smoke-test
    // -----------------------------------------------------------------------

    @Test
    void putCommitsInUnder100Ms() throws Exception {
        long start = System.currentTimeMillis();
        HttpResponse put = request("PUT", "/keys/latency", "v".getBytes());
        long elapsed = System.currentTimeMillis() - start;

        assertThat(put.status).isEqualTo(200);
        assertThat(elapsed).as("committed in under 100 ms on loopback").isLessThan(100);
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private HttpResponse request(String method, String path, byte[] body) throws IOException {
        HttpURLConnection conn = openConn(method, path);
        if (body != null && body.length > 0) {
            conn.setDoOutput(true);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body);
            }
        }
        return readResponse(conn);
    }

    private HttpResponse requestJson(String method, String path, String jsonBody) throws IOException {
        HttpURLConnection conn = openConn(method, path);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);
        try (OutputStream os = conn.getOutputStream()) {
            os.write(jsonBody.getBytes(StandardCharsets.UTF_8));
        }
        return readResponse(conn);
    }

    private HttpURLConnection openConn(String method, String path) throws IOException {
        HttpURLConnection conn = (HttpURLConnection)
            URI.create("http://127.0.0.1:" + httpPort + path).toURL().openConnection();
        conn.setRequestMethod(method);
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(10_000);
        conn.setInstanceFollowRedirects(false);
        return conn;
    }

    private static HttpResponse readResponse(HttpURLConnection conn) throws IOException {
        int status = conn.getResponseCode();
        InputStream is = status >= 400 ? conn.getErrorStream() : conn.getInputStream();
        byte[] rawBody = is == null ? new byte[0] : is.readAllBytes();
        return new HttpResponse(status, rawBody);
    }

    private static String casJson(String expected, String newValue) {
        String enc = Base64.getEncoder().encodeToString(
            expected.getBytes(StandardCharsets.UTF_8));
        String encNew = Base64.getEncoder().encodeToString(
            newValue.getBytes(StandardCharsets.UTF_8));
        return "{\"expected\":\"" + enc + "\",\"value\":\"" + encNew + "\"}";
    }

    private static int findFreePort() throws IOException {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            s.setReuseAddress(true);
            return s.getLocalPort();
        }
    }

    record HttpResponse(int status, byte[] rawBody) {
        String body() { return new String(rawBody, StandardCharsets.UTF_8); }
    }
}
