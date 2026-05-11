package io.kronos.server.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import io.kronos.common.config.PeerConfig;
import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.raft.core.RaftNode;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Handles the /cluster context:
 *
 * <ul>
 *   <li>{@code GET /cluster}     — full cluster state (peers, indices, roles)</li>
 *   <li>{@code GET /cluster/log} — up to 50 recent committed log entries (debugging)</li>
 * </ul>
 */
final class ClusterInfoHandler implements HttpHandler {

    private static final int LOG_LIMIT = 50;

    private final RaftNode         raft;
    private final List<PeerConfig> peerConfigs;

    ClusterInfoHandler(RaftNode raft, List<PeerConfig> peerConfigs) {
        this.raft        = raft;
        this.peerConfigs = peerConfigs;
    }

    @Override
    public void handle(HttpExchange ex) throws IOException {
        try {
            if (!"GET".equals(ex.getRequestMethod())) {
                KeysHandler.sendText(ex, 405, "Method Not Allowed");
                return;
            }
            String path = ex.getRequestURI().getPath();
            if (path.startsWith("/cluster/log")) {
                handleLog(ex);
            } else {
                KeysHandler.sendJson(ex, 200, buildClusterJson());
            }
        } finally {
            ex.close();
        }
    }

    // -----------------------------------------------------------------------
    // /cluster/log
    // -----------------------------------------------------------------------

    private void handleLog(HttpExchange ex) throws IOException {
        List<LogEntry> entries;
        try {
            entries = raft.recentEntries(LOG_LIMIT).get(2, SECONDS);
        } catch (ExecutionException | TimeoutException | InterruptedException e) {
            Thread.currentThread().interrupt();
            KeysHandler.sendText(ex, 503, "{\"error\":\"cluster unavailable\"}");
            return;
        }

        StringBuilder sb = new StringBuilder(512);
        sb.append("{\"nodeId\":\"").append(esc(raft.id().value())).append('"');
        sb.append(",\"commitIndex\":").append(raft.commitIndex().value());
        sb.append(",\"entries\":[");

        boolean first = true;
        for (LogEntry e : entries) {
            if (!first) sb.append(',');
            first = false;
            sb.append("{\"index\":").append(e.index().value());
            sb.append(",\"term\":").append(e.term().value());
            sb.append(",\"payloadBytes\":").append(e.payloadLength());
            // Base64-encode payload so the JSON stays valid for binary commands
            sb.append(",\"payload\":\"")
              .append(Base64.getEncoder().encodeToString(e.payload()))
              .append('"');
            sb.append('}');
        }
        sb.append("]}");
        KeysHandler.sendJson(ex, 200, sb.toString());
    }

    // -----------------------------------------------------------------------
    // /cluster
    // -----------------------------------------------------------------------

    private String buildClusterJson() {
        NodeId        leader = raft.currentLeader();
        long          nowMs  = System.currentTimeMillis();
        StringBuilder sb     = new StringBuilder(512);

        sb.append("{\"nodeId\":\"").append(esc(raft.id().value())).append('"');
        sb.append(",\"role\":\"").append(raft.role()).append('"');
        sb.append(",\"leaderId\":").append(
            leader != null ? "\"" + esc(leader.value()) + "\"" : "null");
        sb.append(",\"term\":").append(raft.currentTerm().value());
        sb.append(",\"commitIndex\":").append(raft.commitIndex().value());
        sb.append(",\"lastApplied\":").append(raft.lastApplied().value());
        sb.append(",\"peers\":[");

        boolean first = true;
        for (PeerConfig pc : peerConfigs) {
            if (!first) sb.append(',');
            first = false;

            NodeId   peerId   = pc.nodeId();
            LogIndex match    = raft.matchIndex(peerId);
            LogIndex next     = raft.nextIndex(peerId);
            long     lastSeen = raft.peerLastSeenMs(peerId);
            long     lagMs    = lastSeen < 0 ? -1 : nowMs - lastSeen;

            sb.append("{\"nodeId\":\"").append(esc(peerId.value())).append('"');
            sb.append(",\"host\":\"").append(esc(pc.host())).append('"');
            sb.append(",\"raftPort\":").append(pc.raftPort());
            sb.append(",\"matchIndex\":").append(match.value());
            sb.append(",\"nextIndex\":").append(next.value());
            sb.append(",\"lastSeenMs\":").append(lastSeen);
            sb.append(",\"lagMs\":").append(lagMs);
            sb.append('}');
        }

        sb.append("]}");
        return sb.toString();
    }

    private static String esc(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
