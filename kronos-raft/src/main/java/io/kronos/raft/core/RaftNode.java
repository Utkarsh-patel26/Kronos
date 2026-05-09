package io.kronos.raft.core;

import io.kronos.common.log.KronosLogger;
import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.codec.AppendEntriesCodec;
import io.kronos.network.codec.RequestVoteCodec;
import io.kronos.network.protocol.MessageType;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.network.rpc.message.AppendEntriesRequest;
import io.kronos.network.rpc.message.AppendEntriesResponse;
import io.kronos.network.rpc.message.RequestVoteRequest;
import io.kronos.network.rpc.message.RequestVoteResponse;
import io.kronos.raft.election.ElectionTimer;
import io.kronos.raft.election.VoteTracker;
import io.kronos.raft.log.RaftLog;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

import static io.kronos.raft.core.RaftRole.*;
import static java.nio.file.StandardOpenOption.*;

/**
 * Core Raft state machine. All mutable state is owned by this class and
 * MUST only be read or written from the single raft thread passed at
 * construction. The only exceptions are the volatile accessors used by tests
 * and monitoring — those are safe to read from any thread.
 */
public final class RaftNode {

    private static final KronosLogger log = KronosLogger.forClass(RaftNode.class);

    private static final int MAX_BATCH_SIZE = 100;

    // Persistent state — written to disk before responding to any RPC
    private volatile Term   currentTerm = Term.ZERO;
    private volatile NodeId votedFor    = null;

    // Volatile Raft state
    private volatile RaftRole role          = FOLLOWER;
    private volatile NodeId   currentLeader = null;
    private volatile LogIndex commitIndex   = LogIndex.ZERO;
    private          LogIndex lastApplied   = LogIndex.ZERO;

    // Leader-only state, reset on every election win
    private final Map<NodeId, LogIndex> nextIndex  = new HashMap<>();
    private final Map<NodeId, LogIndex> matchIndex = new HashMap<>();

    // Pending client writes — keyed by log index, completed when entry is committed
    private final Map<LogIndex, CompletableFuture<LogIndex>> pendingCommits = new HashMap<>();

    private final RaftLog                  raftLog;
    private final StateMachine             stateMachine;
    private final ElectionTimer            electionTimer;
    private final RpcDispatcher            rpcDispatcher;
    private final Map<NodeId, RpcChannel>  rpcChannels;
    private final List<NodeId>             peers;
    private final NodeId                   myId;
    private final Path                     dataDir;
    private final int                      heartbeatIntervalMs;
    private final ScheduledExecutorService raftThread;

    private ScheduledFuture<?> heartbeatFuture;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /** Convenience constructor — uses a no-op state machine. */
    public RaftNode(NodeId myId,
                    List<NodeId> peers,
                    Map<NodeId, RpcChannel> rpcChannels,
                    RpcDispatcher rpcDispatcher,
                    RaftLog raftLog,
                    Path dataDir,
                    int electionTimeoutMinMs,
                    int electionTimeoutMaxMs,
                    int heartbeatIntervalMs,
                    ScheduledExecutorService raftThread) {
        this(myId, peers, rpcChannels, rpcDispatcher, raftLog, dataDir,
             electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
             raftThread, (idx, cmd) -> {});
    }

    public RaftNode(NodeId myId,
                    List<NodeId> peers,
                    Map<NodeId, RpcChannel> rpcChannels,
                    RpcDispatcher rpcDispatcher,
                    RaftLog raftLog,
                    Path dataDir,
                    int electionTimeoutMinMs,
                    int electionTimeoutMaxMs,
                    int heartbeatIntervalMs,
                    ScheduledExecutorService raftThread,
                    StateMachine stateMachine) {
        this.myId               = myId;
        this.peers              = List.copyOf(peers);
        this.rpcChannels        = Map.copyOf(rpcChannels);
        this.rpcDispatcher      = rpcDispatcher;
        this.raftLog            = raftLog;
        this.stateMachine       = stateMachine;
        this.dataDir            = dataDir;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.raftThread         = raftThread;

        this.electionTimer = new ElectionTimer(
            electionTimeoutMinMs, electionTimeoutMaxMs, raftThread, this::onElectionTimeout);

        loadPersistedState();
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    public void start() {
        rpcDispatcher.register(MessageType.REQUEST_VOTE_REQUEST, body -> {
            RequestVoteRequest req = RequestVoteCodec.decodeRequest(body);
            RequestVoteResponse resp = handleRequestVote(req);
            return RequestVoteCodec.encodeResponse(resp);
        });
        rpcDispatcher.register(MessageType.APPEND_ENTRIES_REQUEST, body -> {
            AppendEntriesRequest req = AppendEntriesCodec.decodeRequest(body);
            AppendEntriesResponse resp = handleAppendEntries(req);
            return AppendEntriesCodec.encodeResponse(resp);
        });
        electionTimer.reset();
        log.info("node %s started (term=%d, role=%s)", myId.value(), currentTerm.value(), role);
    }

    public void stop() {
        electionTimer.stop();
        stopHeartbeating();
        raftThread.shutdownNow();
    }

    // -----------------------------------------------------------------------
    // Client write API — safe to call from any thread
    // -----------------------------------------------------------------------

    /**
     * Appends a command to the log and returns a future that completes with
     * the committed log index once a majority has acknowledged it.
     * Fails with {@link NotLeaderException} if this node is not the leader.
     */
    public CompletableFuture<LogIndex> appendCommand(byte[] command) {
        CompletableFuture<LogIndex> future = new CompletableFuture<>();
        try {
            raftThread.execute(() -> {
                if (role != LEADER) {
                    future.completeExceptionally(new NotLeaderException(currentLeader));
                    return;
                }
                LogIndex newIndex = raftLog.lastEntry().index().increment();
                LogEntry entry    = new LogEntry(currentTerm, newIndex, command);
                raftLog.append(entry);
                pendingCommits.put(newIndex, future);
                for (NodeId peer : peers) {
                    replicateTo(peer);
                }
                // Single-node cluster: no peers, advance commit immediately
                advanceCommitIndex();
            });
        } catch (RejectedExecutionException e) {
            // Raft thread shut down — node is no longer accepting writes
            future.completeExceptionally(new NotLeaderException(currentLeader));
        }
        return future;
    }

    // -----------------------------------------------------------------------
    // Role transitions — every role change goes through here
    // -----------------------------------------------------------------------

    private void transitionTo(RaftRole newRole) {
        if (role == newRole) return;
        log.info("node %s: %s -> %s (term=%d)", myId.value(), role, newRole, currentTerm.value());
        role = newRole;

        switch (newRole) {
            case FOLLOWER  -> {
                electionTimer.reset();
                currentLeader = null;
                stopHeartbeating();
                failPendingCommits();
            }
            case CANDIDATE -> startElection();
            case LEADER    -> { electionTimer.stop(); initializeLeaderState(); startHeartbeating(); }
        }
    }

    // -----------------------------------------------------------------------
    // Election
    // -----------------------------------------------------------------------

    private void onElectionTimeout() {
        if (role == LEADER) return;
        if (role == CANDIDATE) {
            // Timer fired while already a candidate — split vote or timeout; start a new election.
            startElection();
        } else {
            transitionTo(CANDIDATE);
        }
    }

    private void startElection() {
        currentTerm = currentTerm.increment();
        votedFor    = myId;
        persistTermAndVote();

        log.info("node %s starting election for term %d", myId.value(), currentTerm.value());
        electionTimer.reset();

        VoteTracker tracker = new VoteTracker(peers.size() + 1);
        tracker.recordVote(myId, true);

        if (tracker.hasWon()) {
            transitionTo(LEADER);
            return;
        }

        LogEntry lastEntry    = raftLog.lastEntry();
        Term     electionTerm = currentTerm;

        for (NodeId peer : peers) {
            RpcChannel channel = rpcChannels.get(peer);
            if (channel == null) continue;

            RequestVoteRequest req = new RequestVoteRequest(
                currentTerm, myId, lastEntry.index(), lastEntry.term());

            channel.send(
                    MessageType.REQUEST_VOTE_REQUEST,
                    RequestVoteCodec.encodeRequest(req),
                    frame -> RequestVoteCodec.decodeResponse(frame.body()))
                .thenAcceptAsync(
                    resp -> onVoteResponse(peer, resp, tracker, electionTerm), raftThread)
                .exceptionally(ex -> {
                    log.warn("vote rpc to %s failed: %s", peer.value(), ex.getMessage());
                    return null;
                });
        }
    }

    private void onVoteResponse(NodeId peer, RequestVoteResponse resp,
                                VoteTracker tracker, Term electionTerm) {
        if (resp.term().compareTo(currentTerm) > 0) {
            currentTerm = resp.term();
            votedFor    = null;
            persistTermAndVote();
            transitionTo(FOLLOWER);
            return;
        }
        if (role != CANDIDATE || !currentTerm.equals(electionTerm)) return;

        tracker.recordVote(peer, resp.voteGranted());
        if (tracker.hasWon()) {
            transitionTo(LEADER);
        }
    }

    // -----------------------------------------------------------------------
    // Incoming RPCs — called on the raft thread via RpcDispatcher
    // -----------------------------------------------------------------------

    RequestVoteResponse handleRequestVote(RequestVoteRequest req) {
        if (req.term().compareTo(currentTerm) < 0) {
            return new RequestVoteResponse(currentTerm, false);
        }

        if (req.term().compareTo(currentTerm) > 0) {
            currentTerm = req.term();
            votedFor    = null;
            persistTermAndVote();
            if (role != FOLLOWER) transitionTo(FOLLOWER);
        }

        boolean logOk   = isLogUpToDate(req.lastLogTerm(), req.lastLogIndex());
        boolean canVote = (votedFor == null || votedFor.equals(req.candidateId())) && logOk;

        if (canVote) {
            votedFor = req.candidateId();
            persistTermAndVote();
            electionTimer.reset();
            return new RequestVoteResponse(currentTerm, true);
        }
        return new RequestVoteResponse(currentTerm, false);
    }

    AppendEntriesResponse handleAppendEntries(AppendEntriesRequest req) {
        // Rule 1: reject stale leader
        if (req.term().compareTo(currentTerm) < 0) {
            return new AppendEntriesResponse(currentTerm, false, raftLog.lastEntry().index());
        }

        // Rule 2: update term, recognize leader, reset election timer
        if (req.term().compareTo(currentTerm) > 0) {
            currentTerm = req.term();
            votedFor    = null;
            persistTermAndVote();
        }
        if (role != FOLLOWER) transitionTo(FOLLOWER);
        currentLeader = req.leaderId();
        electionTimer.reset();

        // Rule 3: prevLog consistency check
        LogIndex prevIdx = req.prevLogIndex();
        if (prevIdx.value() > raftLog.lastEntry().index().value()) {
            // We don't have the entry the leader thinks we do
            return new AppendEntriesResponse(currentTerm, false, raftLog.lastEntry().index());
        }
        if (prevIdx.value() > 0) {
            LogEntry prevEntry = raftLog.entryAt(prevIdx);
            if (!prevEntry.term().equals(req.prevLogTerm())) {
                return new AppendEntriesResponse(currentTerm, false, raftLog.lastEntry().index());
            }
        }

        // Rule 4: append entries, truncating any conflicts first
        for (LogEntry incoming : req.entries()) {
            LogIndex lastIdx = raftLog.lastEntry().index();
            if (incoming.index().value() <= lastIdx.value()) {
                LogEntry existing = raftLog.entryAt(incoming.index());
                if (!existing.term().equals(incoming.term())) {
                    log.info("node %s truncating log from index %d (conflict at term %d vs %d)",
                        myId.value(), incoming.index().value(),
                        existing.term().value(), incoming.term().value());
                    raftLog.truncateFrom(incoming.index());
                    raftLog.append(incoming);
                }
                // else: already have a matching entry — skip
            } else {
                raftLog.append(incoming);
            }
        }

        // Rule 5: advance commitIndex
        if (req.leaderCommit().compareTo(commitIndex) > 0) {
            LogIndex myLast   = raftLog.lastEntry().index();
            LogIndex newCommit = req.leaderCommit().compareTo(myLast) < 0
                ? req.leaderCommit() : myLast;
            if (newCommit.compareTo(commitIndex) > 0) {
                commitIndex = newCommit;
                applyCommitted();
            }
        }

        return new AppendEntriesResponse(currentTerm, true, raftLog.lastEntry().index());
    }

    // -----------------------------------------------------------------------
    // Log currency check
    // -----------------------------------------------------------------------

    private boolean isLogUpToDate(Term candidateLastTerm, LogIndex candidateLastIndex) {
        LogEntry myLast = raftLog.lastEntry();
        if (candidateLastTerm.compareTo(myLast.term()) != 0) {
            return candidateLastTerm.compareTo(myLast.term()) > 0;
        }
        return candidateLastIndex.compareTo(myLast.index()) >= 0;
    }

    // -----------------------------------------------------------------------
    // Leader duties
    // -----------------------------------------------------------------------

    private void initializeLeaderState() {
        LogIndex nextIdx = raftLog.lastEntry().index().increment();
        nextIndex.clear();
        matchIndex.clear();
        for (NodeId peer : peers) {
            nextIndex.put(peer, nextIdx);
            matchIndex.put(peer, LogIndex.ZERO);
        }
        currentLeader = myId;
        log.info("node %s became leader for term %d", myId.value(), currentTerm.value());
    }

    private void startHeartbeating() {
        heartbeatFuture = raftThread.scheduleAtFixedRate(
            this::sendHeartbeats, 0, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void stopHeartbeating() {
        if (heartbeatFuture != null) {
            heartbeatFuture.cancel(false);
            heartbeatFuture = null;
        }
    }

    /**
     * Heartbeats and replication share the same code path: replicateTo() sends
     * whatever entries the peer is missing (possibly none = pure heartbeat).
     */
    private void sendHeartbeats() {
        if (role != LEADER) {
            stopHeartbeating();
            return;
        }
        for (NodeId peer : peers) {
            replicateTo(peer);
        }
    }

    // -----------------------------------------------------------------------
    // Replication
    // -----------------------------------------------------------------------

    private void replicateTo(NodeId peer) {
        RpcChannel channel = rpcChannels.get(peer);
        if (channel == null) return;

        LogIndex next = nextIndex.getOrDefault(peer, raftLog.lastEntry().index().increment());

        // A follower can report a matchIndex beyond our log (e.g. it has extra entries
        // from a previous leader that we don't have). Cap nextIndex so prevIdx stays
        // within our log and entryAt() doesn't throw.
        LogIndex leaderLast = raftLog.lastEntry().index();
        if (next.compareTo(leaderLast.increment()) > 0) {
            next = leaderLast.increment();
            nextIndex.put(peer, next);
        }

        LogIndex prevIdx = new LogIndex(next.value() - 1);  // safe: next >= 1 always
        LogEntry prev    = raftLog.entryAt(prevIdx);        // returns sentinel for ZERO

        List<LogEntry> all   = raftLog.entriesFrom(next);
        List<LogEntry> batch = all.size() > MAX_BATCH_SIZE ? all.subList(0, MAX_BATCH_SIZE) : all;

        Term capturedTerm = currentTerm;

        AppendEntriesRequest req = new AppendEntriesRequest(
            capturedTerm, myId, prev.index(), prev.term(), batch, commitIndex);

        channel.send(
                MessageType.APPEND_ENTRIES_REQUEST,
                AppendEntriesCodec.encodeRequest(req),
                frame -> AppendEntriesCodec.decodeResponse(frame.body()))
            .thenAcceptAsync(resp -> onReplicationResponse(peer, resp, capturedTerm), raftThread)
            .exceptionally(ex -> {
                log.warn("replication to %s failed: %s", peer.value(), ex.getMessage());
                return null;
            });
    }

    private void onReplicationResponse(NodeId peer,
                                        AppendEntriesResponse resp, Term reqTerm) {
        // Step down if a higher term is observed
        if (resp.term().compareTo(currentTerm) > 0) {
            currentTerm = resp.term();
            votedFor    = null;
            persistTermAndVote();
            transitionTo(FOLLOWER);
            return;
        }

        // Discard stale responses (e.g. from a previous leadership term)
        if (role != LEADER || !currentTerm.equals(reqTerm)) return;

        if (resp.success()) {
            LogIndex newMatch   = resp.matchIndex();
            LogIndex curMatch   = matchIndex.getOrDefault(peer, LogIndex.ZERO);
            if (newMatch.compareTo(curMatch) > 0) {
                matchIndex.put(peer, newMatch);
                nextIndex.put(peer, newMatch.increment());
            }
            advanceCommitIndex();

            // Continue replication if the peer is still behind
            if (nextIndex.get(peer).compareTo(raftLog.lastEntry().index()) <= 0) {
                replicateTo(peer);
            }
        } else {
            // Log inconsistency — back up nextIndex.
            // Use the follower's matchIndex as a hint to skip ahead when it's simply behind.
            LogIndex hint    = resp.matchIndex().increment();
            LogIndex current = nextIndex.getOrDefault(peer, LogIndex.ONE);
            if (hint.compareTo(current) < 0) {
                // Follower is behind — jump directly to the right point
                nextIndex.put(peer, hint);
            } else if (current.value() > 1) {
                // Term conflict — walk back one step at a time
                nextIndex.put(peer, current.decrement());
            }
            replicateTo(peer);
        }
    }

    // -----------------------------------------------------------------------
    // Commit index advancement
    // -----------------------------------------------------------------------

    /**
     * After any matchIndex update, find the highest log index held by a
     * majority of nodes (including the leader itself) and advance commitIndex
     * to it — provided it belongs to the current term (Raft safety rule §5.4.2).
     */
    private void advanceCommitIndex() {
        List<Long> sorted = new ArrayList<>(peers.size() + 1);
        sorted.add(raftLog.lastEntry().index().value()); // leader always has its own entries
        for (NodeId peer : peers) {
            sorted.add(matchIndex.getOrDefault(peer, LogIndex.ZERO).value());
        }
        sorted.sort(Comparator.reverseOrder());

        // Element at index (N/2) is the majority threshold for N nodes
        long majorityMatch = sorted.get(sorted.size() / 2);
        if (majorityMatch == 0) return;

        LogIndex candidate = LogIndex.of(majorityMatch);
        if (candidate.compareTo(commitIndex) > 0
                && raftLog.entryAt(candidate).term().equals(currentTerm)) {
            commitIndex = candidate;
            applyCommitted();
            completePendingCommits();
        }
    }

    // -----------------------------------------------------------------------
    // Apply and pending-commit bookkeeping
    // -----------------------------------------------------------------------

    private void applyCommitted() {
        while (lastApplied.compareTo(commitIndex) < 0) {
            lastApplied = lastApplied.increment();
            LogEntry entry = raftLog.entryAt(lastApplied);
            stateMachine.apply(lastApplied, entry.payload());
        }
    }

    private void completePendingCommits() {
        pendingCommits.entrySet().removeIf(e -> {
            if (e.getKey().compareTo(commitIndex) <= 0) {
                e.getValue().complete(e.getKey());
                return true;
            }
            return false;
        });
    }

    private void failPendingCommits() {
        if (pendingCommits.isEmpty()) return;
        NotLeaderException ex = new NotLeaderException(currentLeader);
        pendingCommits.values().forEach(f -> f.completeExceptionally(ex));
        pendingCommits.clear();
    }

    // -----------------------------------------------------------------------
    // Persistence — currentTerm and votedFor MUST survive crashes
    // -----------------------------------------------------------------------

    private void persistTermAndVote() {
        try {
            Files.createDirectories(dataDir);
        } catch (IOException e) {
            throw new UncheckedIOException("create data dir", e);
        }
        Path metaFile = dataDir.resolve("term.meta");
        try (FileChannel fc = FileChannel.open(metaFile, WRITE, CREATE, TRUNCATE_EXISTING)) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream(64);
            DataOutputStream      dos  = new DataOutputStream(baos);
            dos.writeLong(currentTerm.value());
            if (votedFor != null) {
                dos.writeBoolean(true);
                dos.writeUTF(votedFor.value());
            } else {
                dos.writeBoolean(false);
            }
            dos.flush();
            fc.write(ByteBuffer.wrap(baos.toByteArray()));
            fc.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("persist term.meta", e);
        }
    }

    private void loadPersistedState() {
        Path metaFile = dataDir.resolve("term.meta");
        if (!Files.exists(metaFile)) return;
        try (FileChannel fc = FileChannel.open(metaFile, READ)) {
            ByteBuffer buf = ByteBuffer.allocate((int) fc.size());
            fc.read(buf);
            buf.flip();
            byte[] data = new byte[buf.remaining()];
            buf.get(data);
            DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));
            currentTerm = Term.of(dis.readLong());
            boolean hasVote = dis.readBoolean();
            votedFor = hasVote ? NodeId.of(dis.readUTF()) : null;
            log.info("node %s loaded: term=%d votedFor=%s",
                myId.value(), currentTerm.value(), votedFor != null ? votedFor.value() : "null");
        } catch (IOException e) {
            throw new UncheckedIOException("load term.meta", e);
        }
    }

    // -----------------------------------------------------------------------
    // Accessors — safe to read from any thread (volatile or immutable)
    // -----------------------------------------------------------------------

    public RaftRole role()        { return role; }
    public Term currentTerm()     { return currentTerm; }
    public NodeId votedFor()      { return votedFor; }
    public NodeId currentLeader() { return currentLeader; }
    public NodeId id()            { return myId; }
    public LogIndex commitIndex() { return commitIndex; }
    public int logSize()          { return raftLog.size(); }
}
