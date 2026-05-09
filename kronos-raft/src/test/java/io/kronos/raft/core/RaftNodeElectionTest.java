package io.kronos.raft.core;

import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.network.rpc.message.AppendEntriesRequest;
import io.kronos.network.rpc.message.AppendEntriesResponse;
import io.kronos.common.model.LogIndex;
import io.kronos.raft.log.InMemoryRaftLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

class RaftNodeElectionTest {

    @TempDir
    Path tmpDir;

    // -----------------------------------------------------------------------
    // Election timer firing
    // -----------------------------------------------------------------------

    @Test
    void singleNodeBecomesLeaderAfterTimeout() throws InterruptedException {
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode node = new RaftNode(NodeId.of("solo"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir, 50, 100, 50, t);
        node.start();

        waitFor(() -> node.role() == RaftRole.LEADER, 1000);
        node.stop();

        assertThat(node.role()).isEqualTo(RaftRole.LEADER);
        assertThat(node.currentTerm().value()).isGreaterThan(0);
        assertThat(node.currentLeader()).isEqualTo(NodeId.of("solo"));
    }

    @Test
    void electionIncrementsTerm() throws InterruptedException {
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode node = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir, 50, 100, 50, t);
        node.start();

        waitFor(() -> node.role() == RaftRole.LEADER, 1000);

        assertThat(node.currentTerm()).isEqualTo(Term.of(1));
        node.stop();
    }

    @Test
    void termPersistedToDiskAfterElection() throws Exception {
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode node = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir, 50, 100, 50, t);
        node.start();

        waitFor(() -> node.role() == RaftRole.LEADER, 1000);
        node.stop();

        assertThat(Files.exists(tmpDir.resolve("term.meta"))).isTrue();
        assertThat(Files.size(tmpDir.resolve("term.meta"))).isGreaterThan(0);
    }

    @Test
    void nodeRecoversPreviousTermAfterRestart() throws Exception {
        Path dataDir = tmpDir.resolve("node");

        // Start and elect a leader so term advances
        ScheduledExecutorService t1 = Executors.newSingleThreadScheduledExecutor();
        RaftNode node = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            new RpcDispatcher(t1), new InMemoryRaftLog(),
            dataDir, 50, 100, 50, t1);
        node.start();

        waitFor(() -> node.role() == RaftRole.LEADER, 1000);
        Term termBeforeStop = node.currentTerm();
        node.stop();

        // Re-create node with the same data directory — should restore the persisted term
        ScheduledExecutorService t2 = Executors.newSingleThreadScheduledExecutor();
        RaftNode restarted = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            new RpcDispatcher(t2), new InMemoryRaftLog(),
            dataDir, 60_000, 120_000, 50, t2);

        assertThat(restarted.currentTerm()).isEqualTo(termBeforeStop);
        restarted.stop();
    }

    // -----------------------------------------------------------------------
    // Step-down on higher term AppendEntries
    // -----------------------------------------------------------------------

    @Test
    void stepDownToFollowerOnHigherTermHeartbeat() throws Exception {
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode node = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir, 50, 100, 50, t);
        node.start();

        waitFor(() -> node.role() == RaftRole.LEADER, 1000);

        Term higherTerm = node.currentTerm().increment().increment();
        AppendEntriesRequest heartbeat = new AppendEntriesRequest(
            higherTerm, NodeId.of("newLeader"),
            LogIndex.ZERO, Term.ZERO, List.of(), LogIndex.ZERO);

        AppendEntriesResponse resp = t.submit(() -> node.handleAppendEntries(heartbeat))
            .get(2, TimeUnit.SECONDS);

        assertThat(resp.success()).isTrue();
        assertThat(node.role()).isEqualTo(RaftRole.FOLLOWER);
        assertThat(node.currentTerm()).isEqualTo(higherTerm);
        assertThat(node.currentLeader()).isEqualTo(NodeId.of("newLeader"));

        node.stop();
    }

    @Test
    void followerResetsElectionTimerOnHeartbeat() throws Exception {
        // Verify a follower stays follower (doesn't start election) while receiving heartbeats
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode follower = new RaftNode(NodeId.of("f1"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir, 100, 150, 50, t);
        follower.start(); // timer starts

        // Repeatedly send heartbeats to keep it from timing out
        long deadline = System.currentTimeMillis() + 500;
        while (System.currentTimeMillis() < deadline) {
            AppendEntriesRequest hb = new AppendEntriesRequest(
                Term.of(1), NodeId.of("leader"),
                LogIndex.ZERO, Term.ZERO, List.of(), LogIndex.ZERO);
            t.submit(() -> follower.handleAppendEntries(hb)).get(1, TimeUnit.SECONDS);
            Thread.sleep(40);
        }

        assertThat(follower.role()).isEqualTo(RaftRole.FOLLOWER);
        follower.stop();
    }

    // -----------------------------------------------------------------------

    private static void waitFor(java.util.function.BooleanSupplier condition, long timeoutMs)
        throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
    }
}
