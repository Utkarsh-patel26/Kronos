package io.kronos.server;

import io.kronos.common.config.KronosConfig;
import io.kronos.common.config.PeerConfig;
import io.kronos.common.log.KronosLogger;
import io.kronos.common.model.LogEntry;
import io.kronos.common.model.NodeId;
import io.kronos.network.channel.NioServer;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.raft.core.RaftNode;
import io.kronos.raft.log.PersistentRaftLog;
import io.kronos.raft.log.RaftLog;
import io.kronos.server.http.HttpApiServer;
import io.kronos.server.kv.KvStateMachine;
import io.kronos.storage.snapshot.SnapshotManager;
import io.kronos.storage.snapshot.SnapshotMetadata;
import io.kronos.storage.wal.WriteAheadLog;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Assembles and starts the full Kronos node from a validated {@link KronosConfig}.
 *
 * Start sequence:
 * <ol>
 *   <li>Raft thread (single-threaded scheduled executor)</li>
 *   <li>KV state machine (apply snapshot if one exists on disk)</li>
 *   <li>WAL + PersistentRaftLog</li>
 *   <li>SnapshotManager</li>
 *   <li>NioServer + RpcDispatcher</li>
 *   <li>RpcChannels for every peer</li>
 *   <li>RaftNode</li>
 *   <li>HTTP API server</li>
 *   <li>JVM shutdown hook for clean teardown</li>
 * </ol>
 */
public final class NodeBootstrap {

    private static final KronosLogger log = KronosLogger.forClass(NodeBootstrap.class);

    private final KronosConfig cfg;

    public NodeBootstrap(KronosConfig cfg) {
        this.cfg = cfg;
    }

    public void start() throws IOException {
        log.info("starting node %s  raft=%d  http=%d", cfg.nodeId.value(), cfg.raftPort, cfg.httpPort);

        // 1. Raft executor — all Raft state lives here
        ScheduledExecutorService raftThread = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-" + cfg.nodeId.value());
            t.setDaemon(false);
            return t;
        });

        // 2. State machine
        KvStateMachine sm = new KvStateMachine();

        // 3. Snapshot manager + WAL — restore SM from latest snapshot if one exists
        SnapshotManager snapManager = new SnapshotManager(cfg.dataDir.resolve("snapshots"));
        SnapshotMetadata latestSnap = snapManager.latestSnapshot();

        WriteAheadLog wal = new WriteAheadLog(cfg.dataDir.resolve("wal"));
        RaftLog raftLog;
        if (latestSnap != null) {
            byte[] state = snapManager.loadState(latestSnap);
            sm.installSnapshot(state);
            LogEntry snapBase = new LogEntry(
                latestSnap.lastIncludedTerm(),
                latestSnap.lastIncludedIndex(),
                new byte[0]);
            raftLog = new PersistentRaftLog(wal, snapBase);
            log.info("restored snapshot at index %d", latestSnap.lastIncludedIndex().value());
        } else {
            raftLog = new PersistentRaftLog(wal);
        }

        // 4. Network layer
        RpcDispatcher rpcDispatcher = new RpcDispatcher(raftThread);
        NioServer nioServer = new NioServer(cfg.bindHost, cfg.raftPort, rpcDispatcher::dispatch);
        rpcDispatcher.setWriter(nioServer::enqueueWrite);

        // 5. Outbound RPC channels (one per peer)
        Map<NodeId, RpcChannel> rpcChannels = new HashMap<>();
        for (PeerConfig peer : cfg.peers) {
            rpcChannels.put(peer.nodeId(),
                new RpcChannel(peer.nodeId(), peer.host(), peer.raftPort(), nioServer, raftThread));
        }

        // 6. RaftNode
        List<NodeId> peerIds = cfg.peers.stream()
            .map(PeerConfig::nodeId)
            .toList();

        RaftNode raftNode = new RaftNode(
            cfg.nodeId, peerIds, rpcChannels, rpcDispatcher,
            raftLog, cfg.dataDir,
            cfg.electionTimeoutMinMs, cfg.electionTimeoutMaxMs,
            cfg.heartbeatIntervalMs,
            raftThread,
            sm, snapManager, cfg.snapshotThreshold);

        // 7. Register RPC handlers and arm the election timer BEFORE the NIO
        //    selector starts, so no incoming frame can arrive before handlers exist.
        raftNode.start();

        // 8. Start NIO selector loop in its own thread
        Thread nioThread = new Thread(nioServer, "nio-" + cfg.nodeId.value());
        nioThread.setDaemon(false);
        nioThread.start();

        // 9. Connect outbound channels (async — they retry on failure)
        for (RpcChannel ch : rpcChannels.values()) ch.connect();

        // 10. HTTP API
        HttpApiServer httpApi = new HttpApiServer(
            raftNode, sm, cfg.peers, cfg.peerHttpAddresses, cfg.httpPort);
        httpApi.start();

        log.info("node %s ready  http=:%d", cfg.nodeId.value(), cfg.httpPort);

        // 11. Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("shutting down node %s", cfg.nodeId.value());
            raftNode.stop();
            httpApi.stop();
            nioServer.stop();
            try { wal.close(); } catch (IOException ex) { log.warn("WAL close error: %s", ex.getMessage()); }
        }, "shutdown-" + cfg.nodeId.value()));
    }
}
