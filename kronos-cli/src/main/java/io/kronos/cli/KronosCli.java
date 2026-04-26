package io.kronos.cli;

import io.kronos.common.log.KronosLogger;

/**
 * Command-line client entrypoint. Phase 1 only provides a stub — commands
 * are implemented in Phase 8.
 */
public final class KronosCli {

    private static final KronosLogger LOG = KronosLogger.forClass(KronosCli.class);

    private KronosCli() {}

    public static void main(String[] args) {
        LOG.info("kronos-cli skeleton invoked (phase 1 — commands live in phase 8)");
    }
}
