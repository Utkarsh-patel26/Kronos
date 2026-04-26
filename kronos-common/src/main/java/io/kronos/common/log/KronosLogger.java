package io.kronos.common.log;

import java.io.PrintStream;
import java.time.Instant;
import java.util.Locale;

/**
 * Minimal structured stdout logger. No SLF4J, no Logback — we emit a fixed,
 * machine-parseable line format that the GUI log viewer (Phase 7) can
 * consume without an external library.
 *
 * <p>Line format:
 * <pre>{@code
 *   [<ISO-8601 timestamp>] [<LEVEL>] [<logger-name>] <message>
 * }</pre>
 *
 * <p>The level column is padded to 5 characters so parsers can rely on
 * fixed offsets.
 */
public final class KronosLogger {

    public enum Level {
        DEBUG, INFO, WARN, ERROR;

        String padded() {
            return switch (this) {
                case DEBUG -> "DEBUG";
                case INFO  -> "INFO ";
                case WARN  -> "WARN ";
                case ERROR -> "ERROR";
            };
        }
    }

    private static volatile Level globalLevel = Level.INFO;
    private static volatile PrintStream out = System.out;
    private static volatile PrintStream err = System.err;

    private final String name;

    private KronosLogger(String name) {
        this.name = name;
    }

    public static KronosLogger forClass(Class<?> clazz) {
        return new KronosLogger(clazz.getSimpleName());
    }

    public static KronosLogger named(String name) {
        return new KronosLogger(name);
    }

    public static void setLevel(Level level) {
        globalLevel = level;
    }

    public static Level level() {
        return globalLevel;
    }

    /** Redirect output streams — used by tests to capture lines. */
    public static void setStreams(PrintStream stdout, PrintStream stderr) {
        out = stdout;
        err = stderr;
    }

    public String name() {
        return name;
    }

    public void debug(String msg, Object... args) { log(Level.DEBUG, msg, args, null); }
    public void info(String msg, Object... args)  { log(Level.INFO,  msg, args, null); }
    public void warn(String msg, Object... args)  { log(Level.WARN,  msg, args, null); }

    public void error(String msg, Object... args) { log(Level.ERROR, msg, args, null); }
    public void error(String msg, Throwable t)    { log(Level.ERROR, msg, new Object[0], t); }

    private void log(Level lvl, String msg, Object[] args, Throwable t) {
        if (lvl.ordinal() < globalLevel.ordinal()) return;

        String rendered = args.length == 0 ? msg : String.format(Locale.ROOT, msg, args);
        String line = String.format(
            Locale.ROOT,
            "[%s] [%s] [%s] %s",
            Instant.now(),
            lvl.padded(),
            name,
            rendered);

        PrintStream stream = (lvl == Level.ERROR || lvl == Level.WARN) ? err : out;
        stream.println(line);

        if (t != null) {
            t.printStackTrace(stream);
        }
    }
}
