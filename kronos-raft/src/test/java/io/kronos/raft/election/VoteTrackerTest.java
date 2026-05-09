package io.kronos.raft.election;

import io.kronos.common.model.NodeId;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class VoteTrackerTest {

    private static NodeId n(String id) { return NodeId.of(id); }

    @Test
    void threeNodeClusterNeedsTwoVotes() {
        VoteTracker t = new VoteTracker(3); // majority = 2
        t.recordVote(n("a"), true);
        assertThat(t.hasWon()).isFalse();
        t.recordVote(n("b"), true);
        assertThat(t.hasWon()).isTrue();
    }

    @Test
    void fiveNodeClusterNeedsThreeVotes() {
        VoteTracker t = new VoteTracker(5); // majority = 3
        t.recordVote(n("a"), true);
        t.recordVote(n("b"), true);
        assertThat(t.hasWon()).isFalse();
        t.recordVote(n("c"), true);
        assertThat(t.hasWon()).isTrue();
    }

    @Test
    void singleNodeWinsImmediately() {
        VoteTracker t = new VoteTracker(1);
        t.recordVote(n("a"), true);
        assertThat(t.hasWon()).isTrue();
    }

    @Test
    void losingMajorityOfDenials() {
        VoteTracker t = new VoteTracker(3);
        t.recordVote(n("a"), false);
        assertThat(t.hasLost()).isFalse();
        t.recordVote(n("b"), false);
        assertThat(t.hasLost()).isTrue();
    }

    @Test
    void neitherWonNorLostOnSplit() {
        VoteTracker t = new VoteTracker(4); // majority = 3
        t.recordVote(n("a"), true);
        t.recordVote(n("b"), false);
        assertThat(t.hasWon()).isFalse();
        assertThat(t.hasLost()).isFalse();
    }

    @Test
    void hasNotWonBeforeAnyVotes() {
        VoteTracker t = new VoteTracker(3);
        assertThat(t.hasWon()).isFalse();
        assertThat(t.hasLost()).isFalse();
    }
}
