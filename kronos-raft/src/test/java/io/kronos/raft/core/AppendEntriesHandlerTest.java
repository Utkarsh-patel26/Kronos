package io.kronos.raft.core;

import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.network.rpc.message.AppendEntriesRequest;
import io.kronos.network.rpc.message.AppendEntriesResponse;
import io.kronos.raft.log.InMemoryRaftLog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for the follower-side AppendEntries handler in RaftNode.
 * All calls are dispatched to the raft thread to mirror real execution.
 */
class AppendEntriesHandlerTest {

    @TempDir
    Path tmpDir;

    private ScheduledExecutorService raftThread;
    private InMemoryRaftLog raftLog;
    private RaftNode node;

    @BeforeEach
    void setUp() {
        raftThread = Executors.newSingleThreadScheduledExecutor();
        raftLog    = new InMemoryRaftLog();
        // Very long election timeout so the timer never fires mid-test
        node = new RaftNode(NodeId.of("n1"), List.of(), Map.of(),
            new RpcDispatcher(raftThread), raftLog,
            tmpDir.resolve("n1"), 60_000, 120_000, 50, raftThread);
        node.start();
    }

    @AfterEach
    void tearDown() { node.stop(); }

    // -----------------------------------------------------------------------
    // Term gating
    // -----------------------------------------------------------------------

    @Test
    void rejectsRequestWithStaleTerm() throws Exception {
        accept(ae(5, "ldr", 0, 0, List.of(), 0)); // advance node to term 5

        AppendEntriesResponse resp = appendEntries(ae(3, "ldr", 0, 0, List.of(), 0));

        assertThat(resp.success()).isFalse();
        assertThat(resp.term().value()).isEqualTo(5);
    }

    @Test
    void updatesTermWhenRequestTermIsHigher() throws Exception {
        accept(ae(3, "ldr", 0, 0, List.of(), 0));

        assertThat(node.currentTerm().value()).isEqualTo(3);
    }

    // -----------------------------------------------------------------------
    // Heartbeats
    // -----------------------------------------------------------------------

    @Test
    void acceptsHeartbeatOnEmptyLog() throws Exception {
        AppendEntriesResponse resp = appendEntries(ae(1, "ldr", 0, 0, List.of(), 0));

        assertThat(resp.success()).isTrue();
        assertThat(resp.matchIndex()).isEqualTo(LogIndex.ZERO);
    }

    // -----------------------------------------------------------------------
    // Log appending
    // -----------------------------------------------------------------------

    @Test
    void appendsTwoNewEntries() throws Exception {
        AppendEntriesResponse resp = appendEntries(
            ae(1, "ldr", 0, 0, List.of(entry(1, 1), entry(1, 2)), 0));

        assertThat(resp.success()).isTrue();
        assertThat(resp.matchIndex()).isEqualTo(LogIndex.of(2));
        assertThat(raftLog.size()).isEqualTo(2);
    }

    @Test
    void appendsSubsequentBatch() throws Exception {
        accept(ae(1, "ldr", 0, 0, List.of(entry(1, 1), entry(1, 2)), 0));

        AppendEntriesResponse resp = appendEntries(
            ae(1, "ldr", 2, 1, List.of(entry(1, 3), entry(1, 4)), 0));

        assertThat(resp.success()).isTrue();
        assertThat(raftLog.size()).isEqualTo(4);
    }

    @Test
    void idempotentWhenEntriesAlreadyPresent() throws Exception {
        accept(ae(1, "ldr", 0, 0, List.of(entry(1, 1), entry(1, 2)), 0));

        // Re-send the same entries — should succeed and not duplicate
        AppendEntriesResponse resp = appendEntries(
            ae(1, "ldr", 0, 0, List.of(entry(1, 1), entry(1, 2)), 0));

        assertThat(resp.success()).isTrue();
        assertThat(raftLog.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // Consistency check (prevLogIndex / prevLogTerm)
    // -----------------------------------------------------------------------

    @Test
    void rejectsWhenPrevLogIndexExceedsLogSize() throws Exception {
        AppendEntriesResponse resp = appendEntries(
            ae(1, "ldr", 5, 1, List.of(), 0)); // our log is empty

        assertThat(resp.success()).isFalse();
    }

    @Test
    void rejectsWhenPrevLogTermMismatches() throws Exception {
        // Follower has entry at index 1 with term 1
        accept(ae(2, "ldr", 0, 0, List.of(entry(1, 1)), 0));

        // Leader says prevLogIndex=1, prevLogTerm=2 — but we have term 1 there
        AppendEntriesResponse resp = appendEntries(
            ae(2, "ldr", 1, 2, List.of(entry(2, 2)), 0));

        assertThat(resp.success()).isFalse();
    }

    // -----------------------------------------------------------------------
    // Log repair (conflict truncation)
    // -----------------------------------------------------------------------

    @Test
    void truncatesConflictingEntriesAndAppendsNew() throws Exception {
        // Follower has 3 entries at term 1
        accept(ae(1, "ldr1", 0, 0,
            List.of(entry(1, 1), entry(1, 2), entry(1, 3)), 0));
        assertThat(raftLog.size()).isEqualTo(3);

        // New leader rewrites from index 2 with term 2
        accept(ae(2, "ldr2", 1, 1,
            List.of(entry(2, 2), entry(2, 3), entry(2, 4)), 0));

        assertThat(raftLog.size()).isEqualTo(4);
        assertThat(raftLog.entryAt(LogIndex.of(1)).term()).isEqualTo(Term.of(1)); // unchanged
        assertThat(raftLog.entryAt(LogIndex.of(2)).term()).isEqualTo(Term.of(2));
        assertThat(raftLog.entryAt(LogIndex.of(3)).term()).isEqualTo(Term.of(2));
        assertThat(raftLog.entryAt(LogIndex.of(4)).term()).isEqualTo(Term.of(2));
    }

    @Test
    void doesNotTruncateMatchingEntries() throws Exception {
        // Entries 1-3 at term 1 are correct
        accept(ae(1, "ldr", 0, 0,
            List.of(entry(1, 1), entry(1, 2), entry(1, 3)), 0));

        // Leader re-sends 1-3 plus adds 4; none of 1-3 should be truncated
        accept(ae(1, "ldr", 0, 0,
            List.of(entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4)), 0));

        assertThat(raftLog.size()).isEqualTo(4);
    }

    // -----------------------------------------------------------------------
    // commitIndex advancement
    // -----------------------------------------------------------------------

    @Test
    void advancesCommitIndexToLeaderCommit() throws Exception {
        accept(ae(1, "ldr", 0, 0,
            List.of(entry(1, 1), entry(1, 2)), 2));

        assertThat(node.commitIndex()).isEqualTo(LogIndex.of(2));
    }

    @Test
    void clampsCommitIndexToOwnLogLength() throws Exception {
        // Leader says commit=10 but follower only has 2 entries
        accept(ae(1, "ldr", 0, 0,
            List.of(entry(1, 1), entry(1, 2)), 10));

        assertThat(node.commitIndex()).isEqualTo(LogIndex.of(2));
    }

    @Test
    void doesNotRegressCommitIndex() throws Exception {
        accept(ae(1, "ldr", 0, 0, List.of(entry(1, 1), entry(1, 2)), 2));
        assertThat(node.commitIndex()).isEqualTo(LogIndex.of(2));

        // Stale heartbeat with lower commit — commitIndex must not go back
        accept(ae(1, "ldr", 2, 1, List.of(), 1));
        assertThat(node.commitIndex()).isEqualTo(LogIndex.of(2));
    }

    // -----------------------------------------------------------------------
    // State machine application
    // -----------------------------------------------------------------------

    @Test
    void stateMachineReceivesAppliedEntries() throws Exception {
        List<Long> applied = new ArrayList<>();
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode withSM = new RaftNode(NodeId.of("sm"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir.resolve("sm"), 60_000, 120_000, 50, t,
            (index, cmd) -> applied.add(index.value()));
        withSM.start();

        try {
            t.submit(() -> withSM.handleAppendEntries(
                ae(1, "ldr", 0, 0, List.of(entry(1, 1), entry(1, 2)), 2)))
             .get(2, TimeUnit.SECONDS);

            assertThat(applied).containsExactly(1L, 2L);
        } finally {
            withSM.stop();
        }
    }

    // -----------------------------------------------------------------------
    // Role transitions
    // -----------------------------------------------------------------------

    @Test
    void stepsDownFromLeaderOnHigherTerm() throws Exception {
        // Lone node becomes leader on short timeout
        ScheduledExecutorService t = Executors.newSingleThreadScheduledExecutor();
        RaftNode leader = new RaftNode(NodeId.of("L"), List.of(), Map.of(),
            new RpcDispatcher(t), new InMemoryRaftLog(),
            tmpDir.resolve("L"), 50, 100, 50, t);
        leader.start();

        long deadline = System.currentTimeMillis() + 2000;
        while (leader.role() != RaftRole.LEADER && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(leader.role()).isEqualTo(RaftRole.LEADER);

        Term higherTerm = leader.currentTerm().increment();
        t.submit(() -> leader.handleAppendEntries(
            ae(higherTerm.value(), "new-leader", 0, 0, List.of(), 0)))
         .get(2, TimeUnit.SECONDS);

        assertThat(leader.role()).isEqualTo(RaftRole.FOLLOWER);
        assertThat(leader.currentTerm()).isEqualTo(higherTerm);
        leader.stop();
    }

    @Test
    void recognisesLeaderIdAfterAcceptingAppendEntries() throws Exception {
        accept(ae(1, "ldr42", 0, 0, List.of(), 0));
        assertThat(node.currentLeader()).isEqualTo(NodeId.of("ldr42"));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private AppendEntriesResponse appendEntries(AppendEntriesRequest req) throws Exception {
        return raftThread.submit(() -> node.handleAppendEntries(req)).get(2, TimeUnit.SECONDS);
    }

    private void accept(AppendEntriesRequest req) throws Exception {
        AppendEntriesResponse resp = appendEntries(req);
        assertThat(resp.success())
            .as("expected AppendEntries to succeed for: %s", req)
            .isTrue();
    }

    private static AppendEntriesRequest ae(long term, String leader,
                                            long prevIdx, long prevTerm,
                                            List<LogEntry> entries,
                                            long leaderCommit) {
        return new AppendEntriesRequest(
            Term.of(term), NodeId.of(leader),
            LogIndex.of(prevIdx), Term.of(prevTerm),
            entries, LogIndex.of(leaderCommit));
    }

    private static LogEntry entry(long term, long index) {
        return new LogEntry(Term.of(term), LogIndex.of(index), new byte[0]);
    }
}
