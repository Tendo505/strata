package io.github.tendo505.strata.sstable;

import io.github.tendo505.strata.util.Bytes;

/** What the manifest records about an SSTable: its file number, size and key range. */
public record TableMeta(long number, long size, byte[] smallest, byte[] largest, long entries) {

    public boolean contains(byte[] key) {
        return Bytes.compare(key, smallest) >= 0 && Bytes.compare(key, largest) <= 0;
    }

    /** Whether this table's key range overlaps [from, to] (either bound null = unbounded). */
    public boolean overlaps(byte[] from, byte[] to) {
        return (to == null || Bytes.compare(smallest, to) <= 0) && (from == null || Bytes.compare(largest, from) >= 0);
    }

    public static String fileName(long number) {
        return String.format("%06d.sst", number);
    }

    @Override
    public String toString() {
        return "#" + number + " [" + Bytes.string(smallest) + " .. " + Bytes.string(largest) + "] " + size + " B";
    }
}
