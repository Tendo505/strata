package io.github.tendo505.strata;

import io.github.tendo505.strata.util.Bytes;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recent writes, in memory and sorted: a concurrent skip list, so readers
 * never block the single writer. Deletes are stored as tombstones (they must
 * hide older values in SSTables until compaction drops them).
 */
final class MemTable {
    /** Marks a deleted key. Compared by identity: no stored value is ever this array. */
    static final byte[] TOMBSTONE = new byte[0];

    /** Rough per-entry overhead of the skip list, so the size estimate tracks real memory use. */
    private static final int OVERHEAD = 48;

    private final ConcurrentSkipListMap<byte[], byte[]> map = new ConcurrentSkipListMap<>(Bytes.ORDER);
    private final AtomicLong bytes = new AtomicLong();

    void apply(Entry e) {
        byte[] value = e.value() == null ? TOMBSTONE : e.value();
        byte[] old = map.put(e.key(), value);
        bytes.addAndGet(e.key().length + value.length + OVERHEAD - (old == null ? 0 : e.key().length + old.length + OVERHEAD));
    }

    /** {@code null}: not here (look in older data). {@link #TOMBSTONE}: deleted here. */
    byte[] get(byte[] key) {
        return map.get(key);
    }

    long approximateBytes() {
        return bytes.get();
    }

    boolean isEmpty() {
        return map.isEmpty();
    }

    int size() {
        return map.size();
    }

    /** Entries from {@code from} (inclusive) onwards, tombstones included. */
    Cursor cursor(byte[] from) {
        Iterator<Map.Entry<byte[], byte[]>> it = (from == null ? map : map.tailMap(from, true)).entrySet().iterator();
        return new Cursor() {
            private Map.Entry<byte[], byte[]> current = it.hasNext() ? it.next() : null;

            @Override
            public boolean valid() {
                return current != null;
            }

            @Override
            public byte[] key() {
                return current.getKey();
            }

            @Override
            public byte[] value() {
                return current.getValue() == TOMBSTONE ? null : current.getValue();
            }

            @Override
            public void next() {
                current = it.hasNext() ? it.next() : null;
            }
        };
    }
}
