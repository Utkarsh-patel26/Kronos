package io.kronos.common.config;

import io.kronos.common.model.NodeId;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Static, validated configuration for a single Kronos node.
 *
 * <p>This object is populated once at process startup from a properties file
 * or environment variables, then treated as immutable. Subsequent phases
 * (Raft, network, storage, server) read from it; none of them mutate it.
 */
public final class KronosConfig {

    public static final int DEFAULT_ELECTION_TIMEOUT_MIN_MS = 150;
    public static final int DEFAULT_ELECTION_TIMEOUT_MAX_MS = 300;
    public static final int DEFAULT_HEARTBEAT_INTERVAL_MS = 50;
    public static final int DEFAULT_SNAPSHOT_THRESHOLD = 10_000;

    public final NodeId nodeId;
    public final String bindHost;
    public final int raftPort;
    public final int httpPort;
    public final List<PeerConfig> peers;
    /** Optional map of peer nodeId → "host:httpPort" for HTTP redirect support. */
    public final Map<NodeId, String> peerHttpAddresses;
    public final Path dataDir;
    public final int electionTimeoutMinMs;
    public final int electionTimeoutMaxMs;
    public final int heartbeatIntervalMs;
    public final int snapshotThreshold;

    private KronosConfig(Builder b) {
        this.nodeId = Objects.requireNonNull(b.nodeId, "node.id is required");
        this.bindHost = Objects.requireNonNull(b.bindHost, "bind.host is required");
        this.raftPort = requirePort(b.raftPort, "raft.port");
        this.httpPort = requirePort(b.httpPort, "http.port");
        this.peers = Collections.unmodifiableList(new ArrayList<>(b.peers));
        this.peerHttpAddresses = Collections.unmodifiableMap(new LinkedHashMap<>(b.peerHttpAddresses));
        this.dataDir = Objects.requireNonNull(b.dataDir, "data.dir is required");
        this.electionTimeoutMinMs = requirePositive(b.electionTimeoutMinMs, "election.timeout.min.ms");
        this.electionTimeoutMaxMs = requirePositive(b.electionTimeoutMaxMs, "election.timeout.max.ms");
        this.heartbeatIntervalMs = requirePositive(b.heartbeatIntervalMs, "heartbeat.interval.ms");
        this.snapshotThreshold = requirePositive(b.snapshotThreshold, "snapshot.threshold");

        if (electionTimeoutMinMs > electionTimeoutMaxMs) {
            throw new IllegalArgumentException(
                "election.timeout.min.ms (" + electionTimeoutMinMs
                    + ") must be <= election.timeout.max.ms (" + electionTimeoutMaxMs + ")");
        }
        if (heartbeatIntervalMs >= electionTimeoutMinMs) {
            throw new IllegalArgumentException(
                "heartbeat.interval.ms (" + heartbeatIntervalMs
                    + ") must be strictly less than election.timeout.min.ms ("
                    + electionTimeoutMinMs + ")");
        }
    }

    private static int requirePort(int v, String name) {
        if (v <= 0 || v > 65_535) {
            throw new IllegalArgumentException(name + " out of range: " + v);
        }
        return v;
    }

    private static int requirePositive(int v, String name) {
        if (v <= 0) {
            throw new IllegalArgumentException(name + " must be > 0, got " + v);
        }
        return v;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static KronosConfig fromFile(Path path) throws IOException {
        Properties props = new Properties();
        try (var in = java.nio.file.Files.newInputStream(path)) {
            props.load(in);
        }
        return fromProperties(props);
    }

    public static KronosConfig fromEnv() {
        Properties props = new Properties();
        System.getenv().forEach((k, v) -> {
            if (k.startsWith("KRONOS_")) {
                String key = k.substring("KRONOS_".length()).toLowerCase().replace('_', '.');
                props.setProperty(key, v);
            }
        });
        return fromProperties(props);
    }

    public static KronosConfig fromProperties(Properties p) {
        Builder b = builder()
            .nodeId(NodeId.of(required(p, "node.id")))
            .bindHost(required(p, "bind.host"))
            .raftPort(Integer.parseInt(required(p, "raft.port")))
            .httpPort(Integer.parseInt(required(p, "http.port")))
            .dataDir(Paths.get(required(p, "data.dir")))
            .electionTimeoutMinMs(intOrDefault(p, "election.timeout.min.ms", DEFAULT_ELECTION_TIMEOUT_MIN_MS))
            .electionTimeoutMaxMs(intOrDefault(p, "election.timeout.max.ms", DEFAULT_ELECTION_TIMEOUT_MAX_MS))
            .heartbeatIntervalMs(intOrDefault(p, "heartbeat.interval.ms", DEFAULT_HEARTBEAT_INTERVAL_MS))
            .snapshotThreshold(intOrDefault(p, "snapshot.threshold", DEFAULT_SNAPSHOT_THRESHOLD));

        String peersRaw = p.getProperty("peers", "").trim();
        if (!peersRaw.isEmpty()) {
            for (String chunk : peersRaw.split(",")) {
                b.addPeer(parsePeer(chunk.trim()));
            }
        }
        String peersHttpRaw = p.getProperty("peers.http", "").trim();
        if (!peersHttpRaw.isEmpty()) {
            for (String chunk : peersHttpRaw.split(",")) {
                int eq = chunk.indexOf('=');
                if (eq > 0 && eq < chunk.length() - 1) {
                    b.addPeerHttpAddress(NodeId.of(chunk.substring(0, eq).trim()),
                                        chunk.substring(eq + 1).trim());
                }
            }
        }
        return b.build();
    }

    private static PeerConfig parsePeer(String spec) {
        int eq = spec.indexOf('=');
        if (eq <= 0 || eq >= spec.length() - 1) {
            throw new IllegalArgumentException("peer must be 'nodeId=host:port', got: " + spec);
        }
        String id = spec.substring(0, eq).trim();
        String hostPort = spec.substring(eq + 1).trim();
        int colon = hostPort.lastIndexOf(':');
        if (colon <= 0 || colon >= hostPort.length() - 1) {
            throw new IllegalArgumentException("peer host:port malformed: " + hostPort);
        }
        String host = hostPort.substring(0, colon).trim();
        int port;
        try {
            port = Integer.parseInt(hostPort.substring(colon + 1).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("peer port not numeric: " + hostPort, e);
        }
        return new PeerConfig(NodeId.of(id), host, port);
    }

    private static String required(Properties p, String key) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("missing required config: " + key);
        }
        return v.trim();
    }

    private static int intOrDefault(Properties p, String key, int def) {
        String v = p.getProperty(key);
        if (v == null || v.isBlank()) return def;
        return Integer.parseInt(v.trim());
    }

    @Override
    public String toString() {
        return "KronosConfig{" + nodeId
            + " bind=" + bindHost
            + " raft=" + raftPort
            + " http=" + httpPort
            + " peers=" + peers.size()
            + " data=" + dataDir
            + '}';
    }

    public static final class Builder {
        private NodeId nodeId;
        private String bindHost = "0.0.0.0";
        private int raftPort;
        private int httpPort;
        private final List<PeerConfig> peers = new ArrayList<>();
        private final Map<NodeId, String> peerHttpAddresses = new LinkedHashMap<>();
        private Path dataDir;
        private int electionTimeoutMinMs = DEFAULT_ELECTION_TIMEOUT_MIN_MS;
        private int electionTimeoutMaxMs = DEFAULT_ELECTION_TIMEOUT_MAX_MS;
        private int heartbeatIntervalMs = DEFAULT_HEARTBEAT_INTERVAL_MS;
        private int snapshotThreshold = DEFAULT_SNAPSHOT_THRESHOLD;

        public Builder nodeId(NodeId v)               { this.nodeId = v; return this; }
        public Builder bindHost(String v)             { this.bindHost = v; return this; }
        public Builder raftPort(int v)                { this.raftPort = v; return this; }
        public Builder httpPort(int v)                { this.httpPort = v; return this; }
        public Builder dataDir(Path v)                { this.dataDir = v; return this; }
        public Builder addPeer(PeerConfig v)          { this.peers.add(v); return this; }
        public Builder peers(List<PeerConfig> v)      { this.peers.clear(); this.peers.addAll(v); return this; }
        public Builder addPeerHttpAddress(NodeId id, String hostPort) { this.peerHttpAddresses.put(id, hostPort); return this; }
        public Builder electionTimeoutMinMs(int v)    { this.electionTimeoutMinMs = v; return this; }
        public Builder electionTimeoutMaxMs(int v)    { this.electionTimeoutMaxMs = v; return this; }
        public Builder heartbeatIntervalMs(int v)     { this.heartbeatIntervalMs = v; return this; }
        public Builder snapshotThreshold(int v)       { this.snapshotThreshold = v; return this; }

        public KronosConfig build() { return new KronosConfig(this); }
    }
}
