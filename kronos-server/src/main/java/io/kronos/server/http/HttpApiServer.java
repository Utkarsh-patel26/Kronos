package io.kronos.server.http;

import com.sun.net.httpserver.HttpServer;
import io.kronos.common.config.PeerConfig;
import io.kronos.common.model.NodeId;
import io.kronos.raft.core.RaftNode;
import io.kronos.server.kv.KvStateMachine;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * HTTP API server using the JDK built-in {@code com.sun.net.httpserver}.
 * Zero external dependencies — suitable for embedding in a single fat-JAR.
 *
 * <pre>
 * PUT  /keys/{key}          – linearizable write
 * GET  /keys/{key}          – linearizable read
 * DELETE /keys/{key}        – linearizable delete
 * POST /keys/{key}/cas      – atomic compare-and-swap
 * GET  /cluster             – cluster state (peers, indices, roles)
 * GET  /metrics             – Prometheus scrape endpoint
 * </pre>
 */
public final class HttpApiServer {

    private final HttpServer server;

    public HttpApiServer(RaftNode raft,
                         KvStateMachine sm,
                         List<PeerConfig> peerConfigs,
                         Map<NodeId, String> peerHttpAddresses,
                         int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 50);
        server.setExecutor(Executors.newFixedThreadPool(8,
            r -> { Thread t = new Thread(r, "http-api"); t.setDaemon(true); return t; }));

        server.createContext("/keys/",   new KeysHandler(raft, sm, peerHttpAddresses));
        server.createContext("/cluster", new ClusterInfoHandler(raft, peerConfigs));
        server.createContext("/metrics", new MetricsHandler(raft, sm));
    }

    public void start() { server.start(); }

    public void stop()  { server.stop(0); }
}
