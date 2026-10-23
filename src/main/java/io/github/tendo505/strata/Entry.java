package io.github.tendo505.strata;

/** A key and its value; a {@code null} value is a tombstone, meaning the key was deleted. */
public record Entry(byte[] key, byte[] value) {
    public boolean isTombstone() {
        return value == null;
    }
}
