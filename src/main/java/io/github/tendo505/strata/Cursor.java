package io.github.tendo505.strata;

/**
 * A position in a sorted run of entries (a memtable, an SSTable, or a merge of
 * them). Keys come out in increasing unsigned order, each at most once.
 */
public interface Cursor {
    boolean valid();

    byte[] key();

    /** {@code null} for a tombstone. */
    byte[] value();

    void next();
}
