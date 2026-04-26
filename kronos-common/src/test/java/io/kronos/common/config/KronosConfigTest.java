package io.kronos.common.config;

import io.kronos.common.model.NodeId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KronosConfigTest {

    private static Properties baseProps() {
        Properties p = new Properties();
        p.setProperty("node.id", "n1");
        p.setProperty("bind.host", "0.0.0.0");
        p.setProperty("raft.port", "9001");
        p.setProperty("http.port", "8001");
        p.setProperty("data.dir", "/tmp/kronos");
        return p;
    }

    @Test
    void loadsMinimalProperties() {
        KronosConfig cfg = KronosConfig.fromProperties(baseProps());
        assertThat(cfg.nodeId).isEqualTo(NodeId.of("n1"));
        assertThat(cfg.bindHost).isEqualTo("0.0.0.0");
        assertThat(cfg.raftPort).isEqualTo(9001);
        assertThat(cfg.httpPort).isEqualTo(8001);
        assertThat(cfg.dataDir).isEqualTo(Paths.get("/tmp/kronos"));
        assertThat(cfg.peers).isEmpty();
        assertThat(cfg.electionTimeoutMinMs).isEqualTo(150);
        assertThat(cfg.electionTimeoutMaxMs).isEqualTo(300);
        assertThat(cfg.heartbeatIntervalMs).isEqualTo(50);
        assertThat(cfg.snapshotThreshold).isEqualTo(10_000);
    }

    @Test
    void parsesPeersList() {
        Properties p = baseProps();
        p.setProperty("peers", "n2=host2:9002, n3=host3:9003");

        KronosConfig cfg = KronosConfig.fromProperties(p);

        assertThat(cfg.peers).hasSize(2);
        assertThat(cfg.peers.get(0))
            .isEqualTo(new PeerConfig(NodeId.of("n2"), "host2", 9002));
        assertThat(cfg.peers.get(1))
            .isEqualTo(new PeerConfig(NodeId.of("n3"), "host3", 9003));
    }

    @Test
    void peersListIsImmutable() {
        KronosConfig cfg = KronosConfig.fromProperties(baseProps());
        assertThatThrownBy(() -> cfg.peers.add(new PeerConfig(NodeId.of("nX"), "h", 1)))
            .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void overridesDefaultTimeouts() {
        Properties p = baseProps();
        p.setProperty("election.timeout.min.ms", "200");
        p.setProperty("election.timeout.max.ms", "400");
        p.setProperty("heartbeat.interval.ms", "75");
        p.setProperty("snapshot.threshold", "500");

        KronosConfig cfg = KronosConfig.fromProperties(p);

        assertThat(cfg.electionTimeoutMinMs).isEqualTo(200);
        assertThat(cfg.electionTimeoutMaxMs).isEqualTo(400);
        assertThat(cfg.heartbeatIntervalMs).isEqualTo(75);
        assertThat(cfg.snapshotThreshold).isEqualTo(500);
    }

    @Test
    void rejectsMissingRequired() {
        Properties p = baseProps();
        p.remove("node.id");
        assertThatThrownBy(() -> KronosConfig.fromProperties(p))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("node.id");
    }

    @Test
    void rejectsInvalidPort() {
        Properties p = baseProps();
        p.setProperty("raft.port", "99999");
        assertThatThrownBy(() -> KronosConfig.fromProperties(p))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMalformedPeer() {
        Properties p = baseProps();
        p.setProperty("peers", "n2-host2:9002");
        assertThatThrownBy(() -> KronosConfig.fromProperties(p))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsHeartbeatGreaterThanElectionMin() {
        Properties p = baseProps();
        p.setProperty("election.timeout.min.ms", "50");
        p.setProperty("heartbeat.interval.ms", "50");
        assertThatThrownBy(() -> KronosConfig.fromProperties(p))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("heartbeat");
    }

    @Test
    void rejectsElectionMinGreaterThanMax() {
        Properties p = baseProps();
        p.setProperty("election.timeout.min.ms", "500");
        p.setProperty("election.timeout.max.ms", "300");
        assertThatThrownBy(() -> KronosConfig.fromProperties(p))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void fromFileRoundTrip(@TempDir Path tempDir) throws IOException {
        Path f = tempDir.resolve("kronos.properties");
        Files.writeString(f, """
            node.id=nA
            bind.host=127.0.0.1
            raft.port=9100
            http.port=8100
            data.dir=/var/kronos
            peers=nB=127.0.0.1:9101,nC=127.0.0.1:9102
            """);

        KronosConfig cfg = KronosConfig.fromFile(f);

        assertThat(cfg.nodeId).isEqualTo(NodeId.of("nA"));
        assertThat(cfg.peers).hasSize(2);
    }

    @Test
    void builderEnforcesSameValidation() {
        assertThatThrownBy(() ->
            KronosConfig.builder()
                .nodeId(NodeId.of("n1"))
                .bindHost("0.0.0.0")
                .raftPort(9001)
                .httpPort(8001)
                .dataDir(Paths.get("/tmp"))
                .heartbeatIntervalMs(500)
                .electionTimeoutMinMs(100)
                .build())
            .isInstanceOf(IllegalArgumentException.class);
    }
}
