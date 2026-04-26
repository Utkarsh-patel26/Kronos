package io.kronos.common.log;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The GUI log viewer in Phase 7 depends on this exact line format. Changes
 * here must be coordinated with the parser there.
 */
class KronosLoggerTest {

    private static final Pattern LINE = Pattern.compile(
        "^\\[[^]]+] \\[(DEBUG|INFO |WARN |ERROR)] \\[[^]]+] .*$");

    private ByteArrayOutputStream outBuf;
    private ByteArrayOutputStream errBuf;
    private KronosLogger.Level originalLevel;

    @BeforeEach
    void redirect() {
        originalLevel = KronosLogger.level();
        outBuf = new ByteArrayOutputStream();
        errBuf = new ByteArrayOutputStream();
        KronosLogger.setStreams(
            new PrintStream(outBuf, true, StandardCharsets.UTF_8),
            new PrintStream(errBuf, true, StandardCharsets.UTF_8));
        KronosLogger.setLevel(KronosLogger.Level.DEBUG);
    }

    @AfterEach
    void restore() {
        KronosLogger.setStreams(System.out, System.err);
        KronosLogger.setLevel(originalLevel);
    }

    @Test
    void infoLineMatchesFormat() {
        KronosLogger log = KronosLogger.named("test");
        log.info("hello %s", "world");

        String line = outBuf.toString(StandardCharsets.UTF_8).trim();
        assertThat(line).matches(LINE);
        assertThat(line).contains("[INFO ]").contains("[test]").contains("hello world");
    }

    @Test
    void warnAndErrorGoToStderr() {
        KronosLogger log = KronosLogger.named("test");
        log.warn("watch out");
        log.error("boom");

        String err = errBuf.toString(StandardCharsets.UTF_8);
        assertThat(err).contains("[WARN ]").contains("watch out");
        assertThat(err).contains("[ERROR]").contains("boom");
        assertThat(outBuf.toString(StandardCharsets.UTF_8)).isEmpty();
    }

    @Test
    void debugOmittedAtInfoLevel() {
        KronosLogger.setLevel(KronosLogger.Level.INFO);
        KronosLogger log = KronosLogger.named("test");
        log.debug("noisy");
        log.info("visible");

        String out = outBuf.toString(StandardCharsets.UTF_8);
        assertThat(out).doesNotContain("noisy").contains("visible");
    }

    @Test
    void errorWithThrowableIncludesStackTrace() {
        KronosLogger log = KronosLogger.named("test");
        log.error("failed", new RuntimeException("root cause"));

        String err = errBuf.toString(StandardCharsets.UTF_8);
        assertThat(err).contains("failed")
            .contains("RuntimeException")
            .contains("root cause");
    }

    @Test
    void forClassUsesSimpleName() {
        KronosLogger log = KronosLogger.forClass(KronosLoggerTest.class);
        assertThat(log.name()).isEqualTo("KronosLoggerTest");
    }
}
