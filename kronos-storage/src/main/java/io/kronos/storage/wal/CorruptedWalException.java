package io.kronos.storage.wal;

public final class CorruptedWalException extends RuntimeException {
    public CorruptedWalException(String message) {
        super(message);
    }
}
