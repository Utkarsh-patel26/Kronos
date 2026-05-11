package io.kronos.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.kronos.raft.core.RaftNode;
import io.kronos.raft.core.RaftRole;
import io.kronos.server.kv.KvStateMachine;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Serves GET /metrics — Prometheus text-format scrape endpoint.
 */
final class MetricsHandler implements HttpHandler {

    private final RaftNode       raft;
    private final KvStateMachine sm;

    MetricsHandler(RaftNode raft, KvStateMachine sm) {
        this.raft = raft;
        this.sm   = sm;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            if (!"GET".equals(ex.getRequestMethod())) {
                KeysHandler.sendText(ex, 405, "Method Not Allowed");
                return;
            }
            byte[] body = buildMetrics().getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().set("Content-Type",
                "text/plain; version=0.0.4; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        } finally {
            ex.close();
        }
    }

    private String buildMetrics() {
        String node = raft.id().value();
        boolean isLeader = RaftRole.LEADER == raft.role();

        return gauge("kronos_current_term",
                     "Current Raft term",
                     node, raft.currentTerm().value())
             + gauge("kronos_commit_index",
                     "Raft commit index",
                     node, raft.commitIndex().value())
             + gauge("kronos_last_applied",
                     "Last applied log index",
                     node, raft.lastApplied().value())
             + gauge("kronos_log_size",
                     "Number of in-memory log entries (uncompacted)",
                     node, raft.logSize())
             + gauge("kronos_kv_keys",
                     "Number of keys in the KV store",
                     node, sm.size())
             + gauge("kronos_is_leader",
                     "1 if this node is the current leader, 0 otherwise",
                     node, isLeader ? 1 : 0);
    }

    private static String gauge(String name, String help, String node, long value) {
        return "# HELP " + name + " " + help + "\n"
             + "# TYPE " + name + " gauge\n"
             + name + "{node=\"" + node + "\"} " + value + "\n";
    }
}
