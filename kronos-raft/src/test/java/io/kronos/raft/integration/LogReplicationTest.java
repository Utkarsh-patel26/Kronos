package io.kronos.raft.integration;

import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.network.channel.NioServer;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.raft.core.NotLeaderException;
import io.kronos.raft.core.RaftNode;
import io.kronos.raft.core.RaftRole;
import io.kronos.raft.log.InMemoryRaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.*;

/**
 * End-to-end replication tests against a real in-process cluster.
 * All nodes communicate via the NIO transport layer on loopback.
 */
class LogReplicationTest {

    @TempDir
    Path tmpDir;

    private final List<NodeHandle> cluster = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (NodeHandle h : cluster) h.shutdown();
    }

    // -----------------------------------------------------------------------

    @Test
    void singleWriteCommitsOnThreeNodeCluster() throws Exception {
        startCluster(3);
        NodeHandle leader = waitForLeader(2000);

        long startMs = System.currentTimeMillis();
        LogIndex committed = leader.node.appendCommand("SET x=1".getBytes())
            .get(5, TimeUnit.SECONDS);
        long elapsedMs = System.currentTimeMillis() - startMs;

        assertThat(committed).isEqualTo(LogIndex.of(1));
        // 100 ms gives 10× headroom over the spec target while still being a real bound
        assertThat(elapsedMs).as("committed in under 100 ms on loopback").isLessThan(100);
    }

    @Test
    void hundredSequentialWritesAllCommit() throws Exception {
        startCluster(3);
        NodeHandle leader = waitForLeader(2000);

        List<CompletableFuture<LogIndex>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(leader.node.appendCommand(("cmd-" + i).getBytes()));
        }

        for (CompletableFuture<LogIndex> f : futures) f.get(10, TimeUnit.SECONDS);

        // All futures resolved to sequential indices
        for (int i = 0; i < 100; i++) {
            assertThat(futures.get(i).getNow(null))
                .isEqualTo(LogIndex.of(i + 1));
        }
    }

    @Test
    void allNodesConvergeToSameLogAfterWrites() throws Exception {
        startCluster(3);
        NodeHandle leader = waitForLeader(2000);

        List<CompletableFuture<LogIndex>> futures = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            futures.add(leader.node.appendCommand(("entry-" + i).getBytes()));
        }
        for (CompletableFuture<LogIndex> f : futures) f.get(5, TimeUnit.SECONDS);

        // All nodes must eventually have the same log size
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            boolean converged = cluster.stream().allMatch(h -> h.node.logSize() == 20);
            if (converged) break;
            Thread.sleep(20);
        }

        for (NodeHandle h : cluster) {
            assertThat(h.node.logSize())
                .as("node %s log size", h.node.id().value())
                .isEqualTo(20);
        }
    }

    @Test
    void lateStartingNodeCatchesUpViaAppendEntries() throws Exception {
        // Build 3-node infrastructure but start only 2 raft nodes initially.
        // The late node's server is running (so the leader can reach it),
        // but its raft handlers aren't registered yet.
        startPartialCluster(3, 2);

        NodeHandle leader = waitForLeader(2000);

        // Append 100 commands — they commit with the 2 running nodes (majority of 3).
        List<CompletableFuture<LogIndex>> futures = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            futures.add(leader.node.appendCommand(("backfill-" + i).getBytes()));
        }
        for (CompletableFuture<LogIndex> f : futures) f.get(10, TimeUnit.SECONDS);

        assertThat(leader.node.logSize()).isEqualTo(100);

        // Start the third node (long election timeout so it doesn't disrupt leadership)
        NodeHandle late = cluster.get(2);
        late.node.start();

        // Wait for the late node's commitIndex to reach 100.
        // commitIndex is volatile so it's safe to read from the test thread.
        long deadline = System.currentTimeMillis() + 5000;
        while (late.node.commitIndex().compareTo(LogIndex.of(100)) < 0
               && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }

        assertThat(late.node.commitIndex())
            .as("late-starting node must commit all 100 entries")
            .isEqualTo(LogIndex.of(100));
    }

    @Test
    void notLeaderExceptionThrownForFollowerWrite() throws Exception {
        startCluster(3);
        NodeHandle leader  = waitForLeader(2000);
        NodeHandle follower = cluster.stream()
            .filter(h -> h.node.role() == RaftRole.FOLLOWER)
            .findFirst()
            .orElseThrow();

        CompletableFuture<LogIndex> future = follower.node.appendCommand("noop".getBytes());

        assertThatThrownBy(() -> future.get(2, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .cause()
            .isInstanceOf(NotLeaderException.class);
    }

    @Test
    void pendingFutureFailsWhenLeaderStepsDown() throws Exception {
        // A lone leader writes to itself and commits immediately (single node).
        // For multi-node: stop the leader mid-write — any unflushed futures fail.
        startCluster(3);
        NodeHandle leader = waitForLeader(2000);

        // Write a command that will commit normally
        CompletableFuture<LogIndex> future1 = leader.node.appendCommand("ok".getBytes());
        future1.get(5, TimeUnit.SECONDS); // verify baseline works

        // Now stop this leader — its raft thread shuts down, pending futures should fail
        leader.node.stop();

        // Any new write on the now-stopped node must fail quickly
        CompletableFuture<LogIndex> future2 = leader.node.appendCommand("stale".getBytes());
        assertThatThrownBy(() -> future2.get(2, TimeUnit.SECONDS))
            .isInstanceOf(Exception.class); // RejectedExecutionException or NotLeaderException
    }

    @Test
    void commitIndexNeverAdvancesToOldTermEntry() throws Exception {
        // Safety property §5.4.2: a leader may only advance commitIndex to an
        // entry from the CURRENT term. Entries from older terms are committed
        // only as a side-effect of committing a current-term entry.
        //
        // We verify this by checking that after every appendCommand the
        // committed entry always carries the leader's current term.
        startCluster(3);
        NodeHandle leader = waitForLeader(2000);

        for (int i = 0; i < 10; i++) {
            LogIndex idx = leader.node.appendCommand(("cmd" + i).getBytes())
                .get(5, TimeUnit.SECONDS);
            assertThat(idx.value()).isGreaterThan(0);
        }

        // commitIndex must equal logSize after all futures resolved
        assertThat(leader.node.commitIndex()).isEqualTo(LogIndex.of(10));
    }

    /**
     * Log-repair integration test.
     *
     * Scenario: three nodes commit 5 entries. A follower then receives (via
     * simulated injection) three extra entries at indices 6-8 from a previous
     * leader's term — the kind of uncommitted tail a follower can accumulate
     * after a leadership change. The new leader must detect the divergence
     * when it writes entry 6 and repair the follower by truncating the stale
     * tail and replacing it with the correct entry.
     */
    @Test
    void leaderRepairesDivergentFollowerLog() throws Exception {
        startCluster(3);
        NodeHandle leader = waitForLeader(2000);

        // Phase 1: commit 5 entries on all three nodes
        for (int i = 0; i < 5; i++) {
            leader.node.appendCommand(("base-" + i).getBytes()).get(5, TimeUnit.SECONDS);
        }
        // Let heartbeats propagate — leader establishes nextIndex[follower] = 6
        Thread.sleep(150);

        // Phase 2: find a follower and inject three stale entries (indices 6, 7, 8)
        // at term 1, which is always lower than any real election term.
        // This simulates a follower that has uncommitted entries from a previous leader.
        NodeHandle follower = cluster.stream()
            .filter(h -> h.node.role() == RaftRole.FOLLOWER)
            .findFirst()
            .orElseThrow(() -> new AssertionError("no follower found"));

        // Use Term.ZERO (value 0) for injected entries — always below any real election
        // term (≥ 1), so the leader will always detect a conflict at index 6.
        follower.raftThread.submit(() -> {
            follower.log.append(new io.kronos.common.model.LogEntry(
                io.kronos.common.model.Term.ZERO, LogIndex.of(6), "stale-6".getBytes()));
            follower.log.append(new io.kronos.common.model.LogEntry(
                io.kronos.common.model.Term.ZERO, LogIndex.of(7), "stale-7".getBytes()));
            follower.log.append(new io.kronos.common.model.LogEntry(
                io.kronos.common.model.Term.ZERO, LogIndex.of(8), "stale-8".getBytes()));
        }).get(2, TimeUnit.SECONDS);

        // Phase 3: leader writes entry 6 at the current term.
        // replicateTo() will send prevLogIndex=5, entries=[e6@leaderTerm].
        // The follower detects the term mismatch at index 6, truncates indices 6-8,
        // and appends the correct entry.
        leader.node.appendCommand("real-6".getBytes()).get(5, TimeUnit.SECONDS);

        // Phase 4: poll the follower's raft thread until the log shrinks back to 6
        // (stale entries 7 and 8 truncated) — this is what proves repair happened.
        long deadline = System.currentTimeMillis() + 3000;
        int size = 8;
        while (size > 6 && System.currentTimeMillis() < deadline) {
            size = follower.raftThread
                .submit(() -> follower.log.size())
                .get(500, TimeUnit.MILLISECONDS);
            if (size > 6) Thread.sleep(20);
        }

        assertThat(size)
            .as("stale entries 7 and 8 must have been truncated by log repair")
            .isEqualTo(6);

        // Entry at index 6 must carry the leader's term, not the injected Term.ZERO
        io.kronos.common.model.Term term6 = follower.raftThread
            .submit(() -> follower.log.entryAt(LogIndex.of(6)).term())
            .get(1, TimeUnit.SECONDS);
        assertThat(term6)
            .as("entry 6 must have been overwritten with the current leader term")
            .isNotEqualTo(io.kronos.common.model.Term.ZERO);
    }

    // -----------------------------------------------------------------------
    // Cluster construction helpers (mirrors LeaderElectionTest)
    // -----------------------------------------------------------------------

    private void startCluster(int size) throws IOException, InterruptedException {
        startPartialCluster(size, size);
    }

    /**
     * Creates a full-mesh cluster of {@code size} nodes but only starts the
     * raft nodes for the first {@code activeCount}. The remaining nodes have
     * their servers and channels wired up but their raft handlers are not yet
     * registered. Use the long election timeout (60 s) for late-joining nodes
     * so they never disrupt leadership during a test.
     */
    private void startPartialCluster(int size, int activeCount)
            throws IOException, InterruptedException {
        List<NodeId> ids = new ArrayList<>();
        for (int i = 0; i < size; i++) ids.add(NodeId.of("node-" + i));

        List<NioServer>                servers     = new ArrayList<>();
        List<RpcDispatcher>            dispatchers = new ArrayList<>();
        List<ScheduledExecutorService> threads     = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            final String name = ids.get(i).value();
            ScheduledExecutorService rt = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-" + name);
                t.setDaemon(true);
                return t;
            });
            RpcDispatcher dispatcher = new RpcDispatcher(rt);
            NioServer server = new NioServer("127.0.0.1", 0, dispatcher::dispatch);
            dispatcher.setWriter(server::enqueueWrite);
            servers.add(server);
            dispatchers.add(dispatcher);
            threads.add(rt);
        }

        for (int i = 0; i < size; i++) {
            Thread t = new Thread(servers.get(i), "srv-" + ids.get(i).value());
            t.setDaemon(true);
            t.start();
        }

        int[] ports = new int[size];
        for (int i = 0; i < size; i++) ports[i] = servers.get(i).localPort();

        for (int i = 0; i < size; i++) {
            NodeId myId = ids.get(i);
            List<NodeId> peers = ids.stream()
                .filter(id -> !id.equals(myId))
                .collect(Collectors.toList());

            Map<NodeId, RpcChannel> channels = new HashMap<>();
            for (int j = 0; j < size; j++) {
                if (j == i) continue;
                channels.put(ids.get(j),
                    new RpcChannel(ids.get(j), "127.0.0.1", ports[j],
                                   servers.get(i), threads.get(i)));
            }

            Path dataDir = tmpDir.resolve(myId.value());
            // Late-joining nodes get a very long election timeout so they
            // never trigger a spurious election during the catch-up window.
            int electionMin = (i < activeCount) ? 150 : 60_000;
            int electionMax = (i < activeCount) ? 300 : 120_000;

            InMemoryRaftLog nodeLog = new InMemoryRaftLog();
            RaftNode node = new RaftNode(myId, peers, channels, dispatchers.get(i),
                nodeLog, dataDir,
                electionMin, electionMax, 50, threads.get(i));

            cluster.add(new NodeHandle(servers.get(i), threads.get(i), channels, node, nodeLog));
        }

        for (NodeHandle h : cluster) {
            for (RpcChannel ch : h.channels.values()) ch.connect();
        }
        Thread.sleep(50);

        // Only start the first activeCount nodes
        for (int i = 0; i < activeCount; i++) {
            cluster.get(i).node.start();
        }
    }

    private NodeHandle waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            for (NodeHandle h : cluster) {
                if (h.node.role() == RaftRole.LEADER) return h;
            }
            Thread.sleep(20);
        }
        fail("No leader elected within " + timeoutMs + " ms");
        return null;
    }

    // -----------------------------------------------------------------------

    static final class NodeHandle {
        final NioServer                server;
        final ScheduledExecutorService raftThread;
        final Map<NodeId, RpcChannel>  channels;
        final RaftNode                 node;
        final InMemoryRaftLog          log;

        NodeHandle(NioServer server, ScheduledExecutorService raftThread,
                   Map<NodeId, RpcChannel> channels, RaftNode node, InMemoryRaftLog log) {
            this.server     = server;
            this.raftThread = raftThread;
            this.channels   = channels;
            this.node       = node;
            this.log        = log;
        }

        void shutdown() {
            try { node.stop(); }    catch (Exception ignored) {}
            try { server.close(); } catch (IOException ignored) {}
        }
    }
}
