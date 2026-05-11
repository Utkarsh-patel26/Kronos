package io.kronos.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.raft.core.NotLeaderException;
import io.kronos.raft.core.RaftNode;
import io.kronos.server.kv.CommandEncoder;
import io.kronos.server.kv.KvStateMachine;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Handles /keys/{key} — GET (linearizable read), PUT (write), DELETE, and
 * POST /keys/{key}/cas (compare-and-swap).
 *
 * Non-leader nodes return 307 with a Location header pointing to the leader's
 * HTTP address when it is known, or an X-Raft-Leader header otherwise.
 */
final class KeysHandler implements HttpHandler {

    private static final String PREFIX = "/keys/";

    private final RaftNode       raft;
    private final KvStateMachine sm;
    private final Map<NodeId, String> peerHttpAddresses;

    KeysHandler(RaftNode raft, KvStateMachine sm, Map<NodeId, String> peerHttpAddresses) {
        this.raft              = raft;
        this.sm                = sm;
        this.peerHttpAddresses = peerHttpAddresses;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            String path = ex.getRequestURI().getPath();
            String rest = path.substring(PREFIX.length()); // everything after "/keys/"

            boolean isCas = rest.endsWith("/cas");
            String key    = isCas ? rest.substring(0, rest.length() - 4) : rest;

            if (key.isEmpty()) {
                sendText(ex, 400, "Key must not be empty");
                return;
            }

            String method = ex.getRequestMethod();
            if (isCas) {
                if ("POST".equals(method)) handleCas(ex, key);
                else sendText(ex, 405, "Method Not Allowed");
            } else {
                switch (method) {
                    case "GET"    -> handleGet(ex, key);
                    case "PUT"    -> handlePut(ex, key);
                    case "DELETE" -> handleDelete(ex, key);
                    default       -> sendText(ex, 405, "Method Not Allowed");
                }
            }
        } finally {
            ex.close();
        }
    }

    // -----------------------------------------------------------------------

    private void handleGet(HttpExchange ex, String key) throws IOException {
        try {
            raft.confirmLeadership().get(2, SECONDS);
        } catch (ExecutionException e) {
            redirectToLeader(ex, e.getCause(), key);
            return;
        } catch (TimeoutException | InterruptedException e) {
            Thread.currentThread().interrupt();
            sendText(ex, 503, "{\"error\":\"cluster unavailable\"}");
            return;
        }

        byte[] value = sm.get(key);
        if (value == null) {
            sendText(ex, 404, "{\"error\":\"not found\"}");
        } else {
            ex.getResponseHeaders().set("Content-Type", "application/octet-stream");
            ex.sendResponseHeaders(200, value.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(value);
            }
        }
    }

    private void handlePut(HttpExchange ex, String key) throws IOException {
        byte[] value   = ex.getRequestBody().readAllBytes();
        byte[] command = CommandEncoder.encodePut(key, value);
        commitAndRespond(ex, command, key);
    }

    private void handleDelete(HttpExchange ex, String key) throws IOException {
        byte[] command = CommandEncoder.encodeDelete(key);
        commitAndRespond(ex, command, key);
    }

    private void handleCas(HttpExchange ex, String key) throws IOException {
        byte[] body = ex.getRequestBody().readAllBytes();
        String json = new String(body, StandardCharsets.UTF_8);

        byte[] expected = extractBase64Field(json, "expected");
        byte[] newValue = extractBase64Field(json, "value");
        if (expected == null || newValue == null) {
            sendText(ex, 400, "{\"error\":\"body must be {\\\"expected\\\":\\\"<base64>\\\",\\\"value\\\":\\\"<base64>\\\"}\"}");
            return;
        }

        byte[] command = CommandEncoder.encodeCas(key, expected, newValue);
        commitAndRespond(ex, command, key);
    }

    // -----------------------------------------------------------------------

    private void commitAndRespond(HttpExchange ex, byte[] command, String key) throws IOException {
        try {
            LogIndex idx = raft.appendCommand(command).get(5, SECONDS);
            sendJson(ex, 200,
                "{\"status\":\"committed\",\"index\":" + idx.value()
                + ",\"leader\":\"" + escape(String.valueOf(raft.id().value())) + "\"}");
        } catch (ExecutionException e) {
            redirectToLeader(ex, e.getCause(), key);
        } catch (TimeoutException | InterruptedException e) {
            Thread.currentThread().interrupt();
            sendText(ex, 503, "{\"error\":\"cluster unavailable\"}");
        }
    }

    private void redirectToLeader(HttpExchange ex, Throwable cause, String key) throws IOException {
        if (cause instanceof NotLeaderException nle) {
            NodeId leader = nle.knownLeader();
            if (leader != null) {
                String httpAddr = peerHttpAddresses.get(leader);
                if (httpAddr != null) {
                    ex.getResponseHeaders().set("Location",
                        "http://" + httpAddr + "/keys/" + key);
                } else {
                    ex.getResponseHeaders().set("X-Raft-Leader", leader.value());
                }
            }
            sendText(ex, 307, "{\"error\":\"not leader\","
                + "\"leader\":\"" + (leader != null ? escape(leader.value()) : "") + "\"}");
        } else {
            sendText(ex, 503, "{\"error\":\"cluster unavailable\"}");
        }
    }

    // -----------------------------------------------------------------------
    // Minimal JSON field extraction for CAS body — avoids any JSON library.
    // Expects {"expected":"<base64>","value":"<base64>"}
    // -----------------------------------------------------------------------

    private static byte[] extractBase64Field(String json, String field) {
        String needle = "\"" + field + "\":\"";
        int start = json.indexOf(needle);
        if (start < 0) return null;
        start += needle.length();
        int end = json.indexOf('"', start);
        if (end < 0) return null;
        try {
            return Base64.getDecoder().decode(json.substring(start, end));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    // -----------------------------------------------------------------------

    static void sendJson(HttpExchange ex, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(bytes);
        }
    }

    static void sendText(HttpExchange ex, int status, String body) throws IOException {
        sendJson(ex, status, body);
    }

    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
