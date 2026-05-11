package io.kronos.raft.integration;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.network.channel.NioServer;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.raft.core.RaftNode;
import io.kronos.raft.core.RaftRole;
import io.kronos.raft.core.SnapshotableStateMachine;
import io.kronos.raft.core.StateMachine;
import io.kronos.raft.log.PersistentRaftLog;
import io.kronos.storage.snapshot.SnapshotManager;
import io.kronos.storage.snapshot.SnapshotMetadata;
import io.kronos.storage.wal.WriteAheadLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Integration tests for WAL persistence and snapshot support.
 * Uses single-node clusters (no peers) to isolate persistence from consensus.
 */
class PersistenceIntegrationTest {

    @TempDir
    Path tmpDir;

    private final List<NodeHandle> cluster = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (NodeHandle h : cluster) h.shutdown();
    }

    // -----------------------------------------------------------------------
    // WAL recovery
    // -----------------------------------------------------------------------

    @Test
    void singleNodePersistsTerm() throws Exception {
        Path dataDir = tmpDir.resolve("node0");
        NodeHandle h = startNode(dataDir, new NoOpStateMachine(), 0);
        waitForLeader(h, 2000);
        long termBefore = h.node.currentTerm().value();
        h.shutdown();
        cluster.clear();

        NodeHandle h2 = startNode(dataDir, new NoOpStateMachine(), 0);
        waitForLeader(h2, 2000);
        assertThat(h2.node.currentTerm().value()).isGreaterThanOrEqualTo(termBefore);
    }

    @Test
    void singleNodePersistsCommittedEntries() throws Exception {
        Path dataDir = tmpDir.resolve("node0");
        CountingStateMachine sm = new CountingStateMachine();
        NodeHandle h = startNode(dataDir, sm, 0);
        waitForLeader(h, 2000);

        for (int i = 0; i < 5; i++) {
            h.node.appendCommand(("cmd-" + i).getBytes()).get(2, TimeUnit.SECONDS);
        }
        h.shutdown();
        cluster.clear();

        CountingStateMachine sm2 = new CountingStateMachine();
        NodeHandle h2 = startNode(dataDir, sm2, 0);
        waitForLeader(h2, 2000);
        // Per Raft §5.4.2, old-term entries are committed only as a side-effect of
        // committing a current-term entry. Append one trigger command to commit all.
        h2.node.appendCommand("trigger".getBytes()).get(2, TimeUnit.SECONDS);
        assertThat(h2.node.commitIndex().value()).isGreaterThanOrEqualTo(5);
    }

    // -----------------------------------------------------------------------
    // Snapshot cycle
    // -----------------------------------------------------------------------

    @Test
    void snapshotTakenWhenLogExceedsThreshold() throws Exception {
        Path dataDir = tmpDir.resolve("node0");
        int threshold = 5;
        KvStateMachine sm = new KvStateMachine();
        NodeHandle h = startNode(dataDir, sm, threshold);
        waitForLeader(h, 2000);

        for (int i = 0; i < threshold + 1; i++) {
            h.node.appendCommand(("k" + i + "=v" + i).getBytes()).get(2, TimeUnit.SECONDS);
        }
        Thread.sleep(200);

        SnapshotManager snapMgr = new SnapshotManager(dataDir.resolve("snapshots"));
        SnapshotMetadata latest = snapMgr.latestSnapshot();
        assertThat(latest).isNotNull();
        assertThat(latest.lastIncludedIndex().value()).isGreaterThan(0);
    }

    @Test
    void restartFromSnapshotRestoresCommitIndex() throws Exception {
        Path dataDir = tmpDir.resolve("node0");
        int threshold = 5;
        KvStateMachine sm = new KvStateMachine();
        NodeHandle h = startNode(dataDir, sm, threshold);
        waitForLeader(h, 2000);

        long lastCommit = 0;
        for (int i = 0; i < threshold + 2; i++) {
            lastCommit = h.node.appendCommand(("k=v" + i).getBytes())
                .get(2, TimeUnit.SECONDS).value();
        }
        Thread.sleep(200);
        h.shutdown();
        cluster.clear();

        KvStateMachine sm2 = new KvStateMachine();
        NodeHandle h2 = startNode(dataDir, sm2, threshold);
        waitForLeader(h2, 2000);
        // Append a trigger in the new term so the recovered WAL entries get committed
        h2.node.appendCommand("trigger".getBytes()).get(2, TimeUnit.SECONDS);
        assertThat(h2.node.commitIndex().value()).isGreaterThanOrEqualTo(lastCommit);
    }

    // -----------------------------------------------------------------------
    // Node factory
    // -----------------------------------------------------------------------

    /**
     * Build and start a single-node cluster backed by WAL + optional snapshots.
     * If the data directory already has a snapshot, it is loaded into {@code sm}
     * before WAL recovery so entries below the snapshot base are skipped.
     *
     * @param snapshotThreshold entries before a snapshot is taken; 0 = disabled
     */
    private NodeHandle startNode(Path dataDir, StateMachine sm, int snapshotThreshold)
            throws IOException {
        Files.createDirectories(dataDir);

        NodeId id = NodeId.of(dataDir.getFileName().toString());
        ScheduledExecutorService rt = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-" + id.value());
            t.setDaemon(true);
            return t;
        });
        RpcDispatcher dispatcher = new RpcDispatcher(rt);
        NioServer server = new NioServer("127.0.0.1", 0, dispatcher::dispatch);
        dispatcher.setWriter(server::enqueueWrite);
        new Thread(server, "srv-" + id.value()) {{ setDaemon(true); }}.start();

        SnapshotManager snapMgr = new SnapshotManager(dataDir.resolve("snapshots"));
        SnapshotMetadata latestSnap = snapMgr.latestSnapshot();

        WriteAheadLog wal = new WriteAheadLog(dataDir.resolve("wal"));
        PersistentRaftLog log;

        if (latestSnap != null && sm instanceof SnapshotableStateMachine ssm) {
            ssm.installSnapshot(snapMgr.loadState(latestSnap));
            LogEntry snapBase = new LogEntry(
                latestSnap.lastIncludedTerm(), latestSnap.lastIncludedIndex(), new byte[0]);
            log = new PersistentRaftLog(wal, snapBase);
        } else {
            log = new PersistentRaftLog(wal);
        }

        RaftNode node;
        if (snapshotThreshold > 0 && sm instanceof SnapshotableStateMachine) {
            node = new RaftNode(id, List.of(), Map.of(), dispatcher, log, dataDir,
                150, 300, 50, rt, sm, snapMgr, snapshotThreshold);
        } else {
            node = new RaftNode(id, List.of(), Map.of(), dispatcher, log, dataDir,
                150, 300, 50, rt, sm);
        }
        node.start();

        NodeHandle h = new NodeHandle(server, rt, wal, node);
        cluster.add(h);
        return h;
    }

    private static void waitForLeader(NodeHandle h, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (h.node.role() == RaftRole.LEADER) return;
            Thread.sleep(20);
        }
        fail("Node did not become leader within " + timeoutMs + " ms");
    }

    // -----------------------------------------------------------------------
    // State machines
    // -----------------------------------------------------------------------

    static final class NoOpStateMachine implements StateMachine {
        @Override public void apply(LogIndex index, byte[] command) {}
    }

    static final class CountingStateMachine implements StateMachine {
        private final AtomicInteger count = new AtomicInteger();
        @Override public void apply(LogIndex index, byte[] command) { count.incrementAndGet(); }
        int applied() { return count.get(); }
    }

    /** Key-value state machine that supports snapshots (key=value\n lines). */
    static final class KvStateMachine implements SnapshotableStateMachine {
        private final Map<String, String> store = new LinkedHashMap<>();

        @Override
        public void apply(LogIndex index, byte[] command) {
            String line = new String(command);
            int eq = line.indexOf('=');
            if (eq > 0) store.put(line.substring(0, eq), line.substring(eq + 1));
        }

        @Override
        public byte[] takeSnapshot() {
            StringBuilder sb = new StringBuilder();
            store.forEach((k, v) -> sb.append(k).append('=').append(v).append('\n'));
            return sb.toString().getBytes();
        }

        @Override
        public void installSnapshot(byte[] snapshot) {
            store.clear();
            for (String line : new String(snapshot).split("\n")) {
                int eq = line.indexOf('=');
                if (eq > 0) store.put(line.substring(0, eq), line.substring(eq + 1));
            }
        }
    }

    // -----------------------------------------------------------------------

    static final class NodeHandle {
        final NioServer                server;
        final ScheduledExecutorService raftThread;
        final WriteAheadLog            wal;
        final RaftNode                 node;

        NodeHandle(NioServer server, ScheduledExecutorService raftThread,
                   WriteAheadLog wal, RaftNode node) {
            this.server     = server;
            this.raftThread = raftThread;
            this.wal        = wal;
            this.node       = node;
        }

        void shutdown() {
            try { node.stop(); }    catch (Exception ignored) {}
            try { wal.close(); }    catch (Exception ignored) {}
            try { server.close(); } catch (Exception ignored) {}
        }
    }
}
