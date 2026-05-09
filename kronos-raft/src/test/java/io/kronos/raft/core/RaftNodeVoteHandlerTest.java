package io.kronos.raft.core;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.network.rpc.message.RequestVoteRequest;
import io.kronos.network.rpc.message.RequestVoteResponse;
import io.kronos.raft.log.InMemoryRaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.*;

class RaftNodeVoteHandlerTest {

    @TempDir
    Path tmpDir;

    private ScheduledExecutorService raftThread;
    private RaftNode node;

    @BeforeEach
    void setUp() {
        raftThread = Executors.newSingleThreadScheduledExecutor();
        RpcDispatcher dispatcher = new RpcDispatcher(raftThread);
        // Long timeouts so the election timer never fires mid-test
        node = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            dispatcher, new InMemoryRaftLog(),
            tmpDir.resolve("n1"), 60_000, 120_000, 50, raftThread);
        node.start();
    }

    @AfterEach
    void tearDown() {
        node.stop();
    }

    // -----------------------------------------------------------------------

    @Test
    void grantsFirstVoteInNewTerm() throws Exception {
        RequestVoteResponse resp = vote(req(1, "c1", 0, 0));
        assertThat(resp.voteGranted()).isTrue();
        assertThat(resp.term()).isEqualTo(Term.of(1));
    }

    @Test
    void rejectsVoteFromStaleTerm() throws Exception {
        vote(req(3, "c1", 0, 0)); // advance to term 3

        RequestVoteResponse resp = vote(req(1, "c2", 0, 0));
        assertThat(resp.voteGranted()).isFalse();
        assertThat(resp.term()).isEqualTo(Term.of(3));
    }

    @Test
    void doesNotVoteTwiceInSameTerm() throws Exception {
        vote(req(1, "c1", 0, 0)); // vote for c1

        RequestVoteResponse resp = vote(req(1, "c2", 0, 0));
        assertThat(resp.voteGranted()).isFalse();
    }

    @Test
    void grantsSameCandidateAgainInSameTerm() throws Exception {
        vote(req(1, "c1", 0, 0));

        RequestVoteResponse resp = vote(req(1, "c1", 0, 0));
        assertThat(resp.voteGranted()).isTrue();
    }

    @Test
    void updatesTermAndGrantsVoteWhenCandidateTermIsHigher() throws Exception {
        vote(req(2, "c1", 0, 0)); // set term to 2

        RequestVoteResponse resp = vote(req(5, "c2", 0, 0));
        assertThat(resp.voteGranted()).isTrue();
        assertThat(node.currentTerm()).isEqualTo(Term.of(5));
    }

    @Test
    void rejectsWhenCandidateLogIsBehind() throws Exception {
        // Build a node that has two entries at term 2
        InMemoryRaftLog populated = new InMemoryRaftLog();
        populated.append(new LogEntry(Term.of(2), LogIndex.of(1), new byte[0]));
        populated.append(new LogEntry(Term.of(2), LogIndex.of(2), new byte[0]));

        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode richNode = new RaftNode(NodeId.of("n2"), List.of(), Map.of(),
            new RpcDispatcher(t), populated,
            tmpDir.resolve("n2"), 60_000, 120_000, 50, t);
        richNode.start();

        try {
            // Candidate claims lastLogIndex=1, lastLogTerm=1 — behind richNode's term 2
            RequestVoteRequest stale = new RequestVoteRequest(
                Term.of(3), NodeId.of("c1"), LogIndex.of(1), Term.of(1));
            RequestVoteResponse resp = t.submit(() -> richNode.handleRequestVote(stale))
                .get(2, TimeUnit.SECONDS);

            assertThat(resp.voteGranted()).isFalse();
        } finally {
            richNode.stop();
        }
    }

    @Test
    void grantsVoteWhenCandidateLogIsEquallyFresh() throws Exception {
        InMemoryRaftLog sameLog = new InMemoryRaftLog();
        sameLog.append(new LogEntry(Term.of(1), LogIndex.of(1), new byte[0]));

        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode n = new RaftNode(NodeId.of("n3"), List.of(), Map.of(),
            new RpcDispatcher(t), sameLog,
            tmpDir.resolve("n3"), 60_000, 120_000, 50, t);
        n.start();

        try {
            RequestVoteRequest req = new RequestVoteRequest(
                Term.of(2), NodeId.of("c1"), LogIndex.of(1), Term.of(1));
            RequestVoteResponse resp = t.submit(() -> n.handleRequestVote(req))
                .get(2, TimeUnit.SECONDS);
            assertThat(resp.voteGranted()).isTrue();
        } finally {
            n.stop();
        }
    }

    @Test
    void stepsDownToFollowerWhenHigherTermArrives() throws Exception {
        // Simulate a node that thinks it is leading by going through an election
        // (single-node, no peers → becomes leader right away with a very short timeout)
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode loneLeader = new RaftNode(NodeId.of("n4"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir.resolve("n4"), 50, 100, 50, t);
        loneLeader.start();

        // Wait for it to become leader
        long deadline = System.currentTimeMillis() + 2000;
        while (loneLeader.role() != RaftRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(loneLeader.role()).isEqualTo(RaftRole.LEADER);

        // Deliver a vote request with a higher term — node must step down
        Term higherTerm = loneLeader.currentTerm().increment().increment();
        RequestVoteRequest req = new RequestVoteRequest(
            higherTerm, NodeId.of("challenger"), LogIndex.ZERO, Term.ZERO);
        t.submit(() -> loneLeader.handleRequestVote(req)).get(2, TimeUnit.SECONDS);

        assertThat(loneLeader.role()).isEqualTo(RaftRole.FOLLOWER);
        assertThat(loneLeader.currentTerm()).isEqualTo(higherTerm);

        loneLeader.stop();
    }

    // -----------------------------------------------------------------------

    private RequestVoteResponse vote(RequestVoteRequest req) throws Exception {
        return raftThread.submit(() -> node.handleRequestVote(req)).get(2, TimeUnit.SECONDS);
    }

    private static RequestVoteRequest req(long term, String candidateId,
                                          long lastIndex, long lastTerm) {
        return new RequestVoteRequest(
            Term.of(term), NodeId.of(candidateId),
            LogIndex.of(lastIndex), Term.of(lastTerm));
    }
}
