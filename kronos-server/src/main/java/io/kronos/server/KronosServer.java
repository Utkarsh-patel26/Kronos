package io.kronos.server;

import io.kronos.common.config.KronosConfig;
import io.kronos.common.log.KronosLogger;

import java.io.IOException;
import java.nio.file.Paths;

/**
 * Process entry point. Loads config from a properties file (first CLI arg)
 * or from environment variables (KRONOS_* prefix), then delegates to
 * {@link NodeBootstrap} to assemble and start the full node stack.
 *
 * <pre>
 * Usage:  java -jar kronos-server.jar [config.properties]
 * Or set: KRONOS_NODE_ID, KRONOS_BIND_HOST, KRONOS_RAFT_PORT, KRONOS_HTTP_PORT,
 *         KRONOS_DATA_DIR, KRONOS_PEERS, KRONOS_PEERS_HTTP  (etc.)
 * </pre>
 */
public final class KronosServer {

    private static final KronosLogger LOG = KronosLogger.forClass(KronosServer.class);

    private KronosServer() {}

    public static void main(String[] args) {
        try {
            KronosConfig cfg = args.length > 0
                ? KronosConfig.fromFile(Paths.get(args[0]))
                : KronosConfig.fromEnv();

            LOG.info("loaded config: %s", cfg);
            new NodeBootstrap(cfg).start();
        } catch (IllegalArgumentException e) {
            LOG.info("configuration error: %s", e.getMessage());
            System.exit(1);
        } catch (IOException e) {
            LOG.info("startup failed: %s", e.getMessage());
            System.exit(2);
        }
    }
}
