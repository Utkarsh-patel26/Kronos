package io.kronos.raft.integration;

import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.channel.NioServer;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
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

class LeaderElectionTest {

    @TempDir
    Path tmpDir;

    private final List<NodeHandle> cluster = new ArrayList<>();

    @AfterEach
    void tearDown() {
        for (NodeHandle h : cluster) h.shutdown();
    }

    // -----------------------------------------------------------------------

    @Test
    void threeNodeClusterElectsExactlyOneLeader() throws Exception {
        startCluster(3);

        NodeHandle leader = waitForLeader(4000);

        assertThat(leader).isNotNull();
        assertThat(leadersIn(cluster)).hasSize(1);
    }

    @Test
    void allFollowersRecogniseTheLeader() throws Exception {
        startCluster(3);

        NodeHandle leader = waitForLeader(4000);
        Thread.sleep(200); // let heartbeats reach followers

        for (NodeHandle h : cluster) {
            if (h == leader) continue;
            assertThat(h.node.role()).isEqualTo(RaftRole.FOLLOWER);
            assertThat(h.node.currentLeader()).isEqualTo(leader.node.id());
        }
    }

    @Test
    void reElectsAfterLeaderStopped() throws Exception {
        startCluster(3);

        NodeHandle first = waitForLeader(4000);
        Term termBefore  = first.node.currentTerm();
        first.node.stop(); // stop just the raft node (server stays up for clean TCP behaviour)

        List<NodeHandle> remaining = cluster.stream()
            .filter(h -> h != first)
            .collect(Collectors.toList());

        // Each round: up to 300 ms election timer + 500 ms RPC timeout (to the stopped server
        // which holds the port open but drops frames). 6 s gives ~7 rounds.
        long deadline = System.currentTimeMillis() + 6000;
        NodeHandle newLeader = null;
        while (newLeader == null && System.currentTimeMillis() < deadline) {
            for (NodeHandle h : remaining) {
                if (h.node.role() == RaftRole.LEADER) { newLeader = h; break; }
            }
            if (newLeader == null) Thread.sleep(20);
        }

        assertThat(newLeader)
            .as("new leader must be elected after the previous one stopped")
            .isNotNull();
        assertThat(newLeader.node.currentTerm().value())
            .isGreaterThan(termBefore.value());
    }

    @Test
    void clusterAgreesOnSameTerm() throws Exception {
        startCluster(3);

        waitForLeader(4000);
        Thread.sleep(200);

        List<Long> terms = cluster.stream()
            .map(h -> h.node.currentTerm().value())
            .distinct()
            .toList();

        assertThat(terms).hasSize(1);
    }

    @Test
    void twoNodeClusterElectsLeader() throws Exception {
        startCluster(2);
        assertThat(waitForLeader(2000)).isNotNull();
    }

    /**
     * Safety: at most one leader per term, always.
     * Liveness: a leader must eventually be elected even after split votes.
     *
     * Using a tight 10 ms election-timeout window maximises the probability
     * that two or three nodes fire their timers simultaneously and split the
     * vote. The test then verifies both invariants over a 3-second window.
     */
    @Test
    void splitVoteResolvesToSingleLeaderWithSafetyPreserved() throws Exception {
        // 100–200 ms window: tight enough to produce split-vote rounds, wide enough
        // that randomisation can eventually break ties and elect a leader.
        startCluster(3, 100, 200);

        // Raft safety: at most ONE leader per term.
        // Two leaders in *different* terms can coexist briefly during a transition, so we
        // group by term and assert each group has at most one member.
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            cluster.stream()
                .filter(h -> h.node.role() == RaftRole.LEADER)
                .collect(Collectors.groupingBy(h -> h.node.currentTerm().value()))
                .forEach((term, leaders) ->
                    assertThat(leaders.size())
                        .as("safety violation: two leaders in term %d", term)
                        .isLessThanOrEqualTo(1));
            Thread.sleep(5);
        }

        // Liveness: after 3 s of possibly-split elections, exactly one leader must exist
        assertThat(leadersIn(cluster))
            .as("liveness violation: no leader elected after 3 seconds")
            .hasSize(1);
    }

    // -----------------------------------------------------------------------
    // Cluster construction
    // -----------------------------------------------------------------------

    private void startCluster(int size) throws IOException, InterruptedException {
        startCluster(size, 150, 300);
    }

    /**
     * Build and start an N-node cluster. All nodes bind to ephemeral ports,
     * each node gets a full-mesh of outbound RpcChannels to every peer.
     */
    private void startCluster(int size, int electionMin, int electionMax)
            throws IOException, InterruptedException {
        List<NodeId> ids = new ArrayList<>();
        for (int i = 0; i < size; i++) ids.add(NodeId.of("node-" + i));

        // Phase 1: create one server per node (port 0 → OS assigns port)
        List<NioServer>                servers     = new ArrayList<>();
        List<RpcDispatcher>            dispatchers = new ArrayList<>();
        List<ScheduledExecutorService> threads     = new ArrayList<>();

        for (int i = 0; i < size; i++) {
            final String nodeName = ids.get(i).value();
            ScheduledExecutorService rt = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "raft-" + nodeName);
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

        // Start server threads
        for (int i = 0; i < size; i++) {
            Thread t = new Thread(servers.get(i), "srv-" + ids.get(i).value());
            t.setDaemon(true);
            t.start();
        }

        // Phase 2: create RpcChannels now that all ports are known
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
                    new RpcChannel(ids.get(j), "127.0.0.1", ports[j], servers.get(i), threads.get(i)));
            }

            Path dataDir = tmpDir.resolve(myId.value());
            RaftNode node = new RaftNode(myId, peers, channels, dispatchers.get(i),
                new InMemoryRaftLog(), dataDir, electionMin, electionMax, 50, threads.get(i));

            cluster.add(new NodeHandle(servers.get(i), threads.get(i), channels, node));
        }

        // Phase 3: connect channels, then start nodes
        for (NodeHandle h : cluster) {
            for (RpcChannel ch : h.channels.values()) ch.connect();
        }

        // Brief pause so outbound connects can complete before election timers fire
        Thread.sleep(50);

        for (NodeHandle h : cluster) h.node.start();
    }

    private NodeHandle waitForLeader(long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            List<NodeHandle> leaders = leadersIn(cluster);
            if (!leaders.isEmpty()) return leaders.get(0);
            Thread.sleep(20);
        }
        fail("No leader elected within " + timeoutMs + " ms");
        return null;
    }

    private static List<NodeHandle> leadersIn(List<NodeHandle> nodes) {
        return nodes.stream()
            .filter(h -> h.node.role() == RaftRole.LEADER)
            .collect(Collectors.toList());
    }

    // -----------------------------------------------------------------------

    static final class NodeHandle {
        final NioServer                server;
        final ScheduledExecutorService raftThread;
        final Map<NodeId, RpcChannel>  channels;
        final RaftNode                 node;

        NodeHandle(NioServer server, ScheduledExecutorService raftThread,
                   Map<NodeId, RpcChannel> channels, RaftNode node) {
            this.server     = server;
            this.raftThread = raftThread;
            this.channels   = channels;
            this.node       = node;
        }

        void shutdown() {
            try { node.stop(); }       catch (Exception ignored) {}
            try { server.close(); }    catch (IOException ignored) {}
        }
    }
}
