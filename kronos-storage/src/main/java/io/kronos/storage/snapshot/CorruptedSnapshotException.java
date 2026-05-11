package io.kronos.storage.snapshot;

public final class CorruptedSnapshotException extends RuntimeException {
    public CorruptedSnapshotException(String message) {
        super(message);
    }
}
