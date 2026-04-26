package io.kronos.server;

import io.kronos.common.log.KronosLogger;

/**
 * Node entrypoint. Phase 1 only provides a stub — the full bootstrap
 * sequence is assembled in Phase 5.
 */
public final class KronosServer {

    private static final KronosLogger LOG = KronosLogger.forClass(KronosServer.class);

    private KronosServer() {}

    public static void main(String[] args) {
        LOG.info("kronos-server skeleton invoked (phase 1 — bootstrap lives in phase 5)");
    }
}
