package io.github.tendo505.strata;

import io.github.tendo505.strata.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Several puts and deletes applied atomically: the batch is one WAL record
 * with one checksum, so after a crash either all of it is recovered or none.
 */
public final class WriteBatch {
    private final List<Entry> ops = new ArrayList<>();
    private long bytes;

    public WriteBatch put(byte[] key, byte[] value) {
        if (key == null || value == null) throw new IllegalArgumentException("key and value are required");
        ops.add(new Entry(key.clone(), value.clone()));
        bytes += key.length + value.length;
        return this;
    }

    public WriteBatch delete(byte[] key) {
        if (key == null) throw new IllegalArgumentException("key is required");
        ops.add(new Entry(key.clone(), null));
        bytes += key.length;
        return this;
    }

    public int size() {
        return ops.size();
    }

    public long userBytes() {
        return bytes;
    }

    List<Entry> ops() {
        return ops;
    }

    /** count, then per op: flag (1 = put, 0 = delete), key, and for a put the value; lengths as varints. */
    byte[] encode() {
        int size = 5;
        for (Entry e : ops) {
            size += 1 + 5 + e.key().length;
            if (e.value() != null) size += 5 + e.value().length;
        }
        ByteBuffer out = ByteBuffer.allocate(size);
        Bytes.putVarint(out, ops.size());
        for (Entry e : ops) {
            out.put((byte) (e.value() == null ? 0 : 1));
            Bytes.putVarint(out, e.key().length);
            out.put(e.key());
            if (e.value() != null) {
                Bytes.putVarint(out, e.value().length);
                out.put(e.value());
            }
        }
        byte[] result = new byte[out.position()];
        System.arraycopy(out.array(), 0, result, 0, result.length);
        return result;
    }

    static List<Entry> decode(byte[] record) {
        ByteBuffer in = ByteBuffer.wrap(record);
        int count = (int) Bytes.getVarint(in);
        List<Entry> entries = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            boolean put = in.get() == 1;
            byte[] key = new byte[(int) Bytes.getVarint(in)];
            in.get(key);
            byte[] value = null;
            if (put) {
                value = new byte[(int) Bytes.getVarint(in)];
                in.get(value);
            }
            entries.add(new Entry(key, value));
        }
        return entries;
    }
}
