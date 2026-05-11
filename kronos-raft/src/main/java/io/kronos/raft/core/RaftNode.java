package io.kronos.raft.core;

import io.kronos.common.log.KronosLogger;
import io.kronos.common.model.LogEntry;
import io.kronos.common.model.LogIndex;
import io.kronos.common.model.NodeId;
import io.kronos.common.model.Term;
import io.kronos.network.codec.AppendEntriesCodec;
import io.kronos.network.codec.InstallSnapshotCodec;
import io.kronos.network.codec.RequestVoteCodec;
import io.kronos.network.protocol.MessageType;
import io.kronos.network.rpc.RpcChannel;
import io.kronos.network.rpc.RpcDispatcher;
import io.kronos.network.rpc.message.AppendEntriesRequest;
import io.kronos.network.rpc.message.AppendEntriesResponse;
import io.kronos.network.rpc.message.InstallSnapshotRequest;
import io.kronos.network.rpc.message.InstallSnapshotResponse;
import io.kronos.network.rpc.message.RequestVoteRequest;
import io.kronos.network.rpc.message.RequestVoteResponse;
import io.kronos.raft.election.ElectionTimer;
import io.kronos.raft.election.VoteTracker;
import io.kronos.raft.log.RaftLog;
import io.kronos.storage.snapshot.SnapshotManager;
import io.kronos.storage.snapshot.SnapshotMetadata;

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
    private volatile LogIndex lastApplied   = LogIndex.ZERO;

    // Leader-only state, reset on every election win.
    // ConcurrentHashMap so the HTTP thread can read snapshots safely.
    private final ConcurrentHashMap<NodeId, LogIndex> nextIndex  = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<NodeId, LogIndex> matchIndex = new ConcurrentHashMap<>();
    // Last time we heard from each peer (epoch ms). Updated on every successful replication.
    private final ConcurrentHashMap<NodeId, Long> peerLastSeen = new ConcurrentHashMap<>();

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

    // Snapshot support — both null if this node does not participate in snapshotting
    private final SnapshotManager snapshotManager;
    private final int             snapshotThreshold;

    private ScheduledFuture<?> heartbeatFuture;

    // -----------------------------------------------------------------------
    // Construction
    // -----------------------------------------------------------------------

    /** Convenience constructor — uses a no-op state machine, no snapshot support. */
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

    /** Full constructor with custom state machine, no snapshot support. */
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
        this(myId, peers, rpcChannels, rpcDispatcher, raftLog, dataDir,
             electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
             raftThread, stateMachine, null, 0);
    }

    /** Full constructor with snapshot support. */
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
                    StateMachine stateMachine,
                    SnapshotManager snapshotManager,
                    int snapshotThreshold) {
        this.myId               = myId;
        this.peers              = List.copyOf(peers);
        this.rpcChannels        = Map.copyOf(rpcChannels);
        this.rpcDispatcher      = rpcDispatcher;
        this.raftLog            = raftLog;
        this.stateMachine       = stateMachine;
        this.dataDir            = dataDir;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.raftThread         = raftThread;
        this.snapshotManager    = snapshotManager;
        this.snapshotThreshold  = snapshotThreshold;

        this.electionTimer = new ElectionTimer(
            electionTimeoutMinMs, electionTimeoutMaxMs, raftThread, this::onElectionTimeout);

        loadPersistedState();

        // If the log has a snapshot base, advance lastApplied/commitIndex accordingly
        // so we don't try to re-apply already-snapshotted entries on startup.
        LogIndex snapIdx = raftLog.snapshotIndex();
        if (snapIdx.value() > 0) {
            if (lastApplied.compareTo(snapIdx) < 0) lastApplied = snapIdx;
            if (commitIndex.compareTo(snapIdx) < 0) commitIndex = snapIdx;
        }
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
        rpcDispatcher.register(MessageType.INSTALL_SNAPSHOT_REQUEST, body -> {
            InstallSnapshotRequest req = InstallSnapshotCodec.decodeRequest(body);
            InstallSnapshotResponse resp = handleInstallSnapshot(req);
            return InstallSnapshotCodec.encodeResponse(resp);
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
        LogIndex prevIdx  = req.prevLogIndex();
        LogIndex snapIdx  = raftLog.snapshotIndex();
        if (prevIdx.value() > raftLog.lastEntry().index().value()) {
            return new AppendEntriesResponse(currentTerm, false, raftLog.lastEntry().index());
        }
        if (prevIdx.value() > 0 && prevIdx.value() >= snapIdx.value()) {
            // prevIdx is at or after the snapshot boundary — perform term check
            LogEntry prevEntry = raftLog.entryAt(prevIdx);
            if (!prevEntry.term().equals(req.prevLogTerm())) {
                return new AppendEntriesResponse(currentTerm, false, raftLog.lastEntry().index());
            }
        }
        // if prevIdx < snapIdx the entry is covered by the snapshot; skip term check

        // Rule 4: append entries, truncating any conflicts first
        for (LogEntry incoming : req.entries()) {
            // Skip entries already covered by our snapshot
            if (incoming.index().value() <= snapIdx.value()) continue;

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
            LogIndex myLast    = raftLog.lastEntry().index();
            LogIndex newCommit = req.leaderCommit().compareTo(myLast) < 0
                ? req.leaderCommit() : myLast;
            if (newCommit.compareTo(commitIndex) > 0) {
                commitIndex = newCommit;
                applyCommitted();
            }
        }

        return new AppendEntriesResponse(currentTerm, true, raftLog.lastEntry().index());
    }

    InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest req) {
        // Reject stale snapshots
        if (req.term().compareTo(currentTerm) < 0) {
            return new InstallSnapshotResponse(currentTerm);
        }
        if (req.term().compareTo(currentTerm) > 0) {
            currentTerm = req.term();
            votedFor    = null;
            persistTermAndVote();
        }
        if (role != FOLLOWER) transitionTo(FOLLOWER);
        currentLeader = req.leaderId();
        electionTimer.reset();

        if (snapshotManager == null) {
            return new InstallSnapshotResponse(currentTerm);
        }

        try {
            snapshotManager.writeChunk(req.offset(), req.data());

            if (req.done()) {
                SnapshotMetadata meta = snapshotManager.finalizeChunked(
                    req.lastIncludedIndex(), req.lastIncludedTerm());

                if (stateMachine instanceof SnapshotableStateMachine sm) {
                    sm.installSnapshot(snapshotManager.loadState(meta));
                }

                raftLog.resetToSnapshot(req.lastIncludedIndex(), req.lastIncludedTerm());
                commitIndex = req.lastIncludedIndex();
                lastApplied = req.lastIncludedIndex();
                log.info("node %s installed snapshot at index %d",
                    myId.value(), req.lastIncludedIndex().value());
            }
        } catch (Exception e) {
            log.warn("InstallSnapshot failed: %s", e.getMessage());
        }

        return new InstallSnapshotResponse(currentTerm);
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

        LogIndex leaderLast = raftLog.lastEntry().index();
        if (next.compareTo(leaderLast.increment()) > 0) {
            next = leaderLast.increment();
            nextIndex.put(peer, next);
        }

        // If the peer needs entries that have been compacted into our snapshot, send it
        LogIndex snapIdx = raftLog.snapshotIndex();
        if (snapshotManager != null && snapIdx.value() > 0 && next.value() <= snapIdx.value()) {
            sendSnapshotTo(peer);
            return;
        }

        LogIndex prevIdx = new LogIndex(next.value() - 1);
        LogEntry prev    = raftLog.entryAt(prevIdx);

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

    private void sendSnapshotTo(NodeId peer) {
        SnapshotMetadata snap;
        try {
            snap = snapshotManager.latestSnapshot();
        } catch (Exception e) {
            log.warn("failed to read snapshot metadata for %s: %s", peer.value(), e.getMessage());
            return;
        }
        if (snap == null) return;

        byte[] snapBytes;
        try {
            snapBytes = snapshotManager.readRaw(snap);
        } catch (Exception e) {
            log.warn("failed to read snapshot bytes for %s: %s", peer.value(), e.getMessage());
            return;
        }

        RpcChannel channel = rpcChannels.get(peer);
        if (channel == null) return;

        Term capturedTerm = currentTerm;
        InstallSnapshotRequest req = new InstallSnapshotRequest(
            capturedTerm, myId,
            snap.lastIncludedIndex(), snap.lastIncludedTerm(),
            0, snapBytes, true);

        channel.send(
                MessageType.INSTALL_SNAPSHOT_REQUEST,
                InstallSnapshotCodec.encodeRequest(req),
                frame -> InstallSnapshotCodec.decodeResponse(frame.body()))
            .thenAcceptAsync(
                resp -> onInstallSnapshotResponse(peer, resp, capturedTerm, snap), raftThread)
            .exceptionally(ex -> {
                log.warn("snapshot install to %s failed: %s", peer.value(), ex.getMessage());
                return null;
            });
    }

    private void onInstallSnapshotResponse(NodeId peer, InstallSnapshotResponse resp,
                                            Term reqTerm, SnapshotMetadata snap) {
        if (resp.term().compareTo(currentTerm) > 0) {
            currentTerm = resp.term();
            votedFor    = null;
            persistTermAndVote();
            transitionTo(FOLLOWER);
            return;
        }
        if (role != LEADER || !currentTerm.equals(reqTerm)) return;

        LogIndex snapIdx = snap.lastIncludedIndex();
        matchIndex.put(peer, snapIdx);
        nextIndex.put(peer, snapIdx.increment());
        advanceCommitIndex();
    }

    private void onReplicationResponse(NodeId peer,
                                        AppendEntriesResponse resp, Term reqTerm) {
        if (resp.term().compareTo(currentTerm) > 0) {
            currentTerm = resp.term();
            votedFor    = null;
            persistTermAndVote();
            transitionTo(FOLLOWER);
            return;
        }

        if (role != LEADER || !currentTerm.equals(reqTerm)) return;

        if (resp.success()) {
            peerLastSeen.put(peer, System.currentTimeMillis());
            LogIndex newMatch = resp.matchIndex();
            LogIndex curMatch = matchIndex.getOrDefault(peer, LogIndex.ZERO);
            if (newMatch.compareTo(curMatch) > 0) {
                matchIndex.put(peer, newMatch);
                nextIndex.put(peer, newMatch.increment());
            }
            advanceCommitIndex();

            if (nextIndex.get(peer).compareTo(raftLog.lastEntry().index()) <= 0) {
                replicateTo(peer);
            }
        } else {
            LogIndex hint    = resp.matchIndex().increment();
            LogIndex current = nextIndex.getOrDefault(peer, LogIndex.ONE);
            if (hint.compareTo(current) < 0) {
                nextIndex.put(peer, hint);
            } else if (current.value() > 1) {
                nextIndex.put(peer, current.decrement());
            }
            replicateTo(peer);
        }
    }

    // -----------------------------------------------------------------------
    // Commit index advancement
    // -----------------------------------------------------------------------

    private void advanceCommitIndex() {
        List<Long> sorted = new ArrayList<>(peers.size() + 1);
        sorted.add(raftLog.lastEntry().index().value());
        for (NodeId peer : peers) {
            sorted.add(matchIndex.getOrDefault(peer, LogIndex.ZERO).value());
        }
        sorted.sort(Comparator.reverseOrder());

        long majorityMatch = sorted.get(sorted.size() / 2);
        if (majorityMatch == 0) return;

        LogIndex candidate = LogIndex.of(majorityMatch);
        if (candidate.compareTo(commitIndex) <= 0) return;

        // Safety: only commit entries from the current term
        LogIndex snapIdx = raftLog.snapshotIndex();
        if (candidate.value() <= snapIdx.value()) return; // already in snapshot

        if (raftLog.entryAt(candidate).term().equals(currentTerm)) {
            commitIndex = candidate;
            applyCommitted();
            completePendingCommits();
        }
    }

    // -----------------------------------------------------------------------
    // Apply, snapshot, and pending-commit bookkeeping
    // -----------------------------------------------------------------------

    private void applyCommitted() {
        while (lastApplied.compareTo(commitIndex) < 0) {
            lastApplied = lastApplied.increment();
            LogEntry entry = raftLog.entryAt(lastApplied);
            stateMachine.apply(lastApplied, entry.payload());
        }
        maybeSnapshot();
    }

    private void maybeSnapshot() {
        if (snapshotManager == null) return;
        if (!(stateMachine instanceof SnapshotableStateMachine sm)) return;
        if (raftLog.size() < snapshotThreshold) return;

        try {
            byte[]   state           = sm.takeSnapshot();
            LogEntry lastEntry       = raftLog.entryAt(lastApplied);
            snapshotManager.save(lastApplied, lastEntry.term(), state);
            raftLog.resetToSnapshot(lastApplied, lastEntry.term());
            log.info("node %s snapshotted at index %d (log compacted)",
                myId.value(), lastApplied.value());
        } catch (Exception e) {
            log.warn("snapshot failed: %s", e.getMessage());
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
    public LogIndex lastApplied() { return lastApplied; }
    public List<NodeId> peers()   { return peers; }
    public int logSize()          { return raftLog.size(); }

    /** matchIndex for a peer (leader state); returns ZERO if not leader or peer unknown. */
    public LogIndex matchIndex(NodeId peer) {
        return matchIndex.getOrDefault(peer, LogIndex.ZERO);
    }

    /** nextIndex for a peer (leader state); returns ZERO if not leader or peer unknown. */
    public LogIndex nextIndex(NodeId peer) {
        return nextIndex.getOrDefault(peer, LogIndex.ZERO);
    }

    /** Epoch-ms of the last successful replication to peer; -1 if never seen. */
    public long peerLastSeenMs(NodeId peer) {
        return peerLastSeen.getOrDefault(peer, -1L);
    }

    /**
     * Returns the most recent {@code limit} committed log entries, fetched
     * safely from the Raft thread.  Entries already compacted into a snapshot
     * are not available and will simply not appear.
     */
    public CompletableFuture<List<LogEntry>> recentEntries(int limit) {
        CompletableFuture<List<LogEntry>> f = new CompletableFuture<>();
        try {
            raftThread.execute(() -> {
                LogIndex last    = raftLog.lastEntry().index();
                LogIndex snapIdx = raftLog.snapshotIndex();
                if (last.value() == 0) {
                    f.complete(List.of());
                    return;
                }
                long startVal = Math.max(last.value() - limit + 1,
                                         snapIdx.value() + 1);
                if (startVal < 1) startVal = 1;
                List<LogEntry> entries = raftLog.entriesFrom(LogIndex.of(startVal));
                if (entries.size() > limit) {
                    entries = entries.subList(entries.size() - limit, entries.size());
                }
                f.complete(List.copyOf(entries));
            });
        } catch (RejectedExecutionException e) {
            f.completeExceptionally(e);
        }
        return f;
    }

    /**
     * Returns a future that completes normally if this node is still the leader
     * when the check executes on the Raft thread, or fails with
     * {@link NotLeaderException} if not.  Used by the HTTP layer to confirm
     * leadership before serving linearizable reads.
     */
    public CompletableFuture<Void> confirmLeadership() {
        CompletableFuture<Void> f = new CompletableFuture<>();
        try {
            raftThread.execute(() -> {
                if (role == LEADER) f.complete(null);
                else f.completeExceptionally(new NotLeaderException(currentLeader));
            });
        } catch (RejectedExecutionException e) {
            f.completeExceptionally(new NotLeaderException(currentLeader));
        }
        return f;
    }
}
