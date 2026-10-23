package io.github.tendo505.strata;

import io.github.tendo505.strata.sstable.TableMeta;
import io.github.tendo505.strata.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * One change to the set of tables (a flush adds one; a compaction removes its
 * inputs and adds its outputs), recorded in the manifest so that the set can
 * be rebuilt after a restart.
 */
final class VersionEdit {
    record Added(int level, TableMeta meta) {}

    record Removed(int level, long number) {}

    final List<Added> added = new ArrayList<>();
    final List<Removed> removed = new ArrayList<>();
    /** File numbers below this are taken. */
    long nextFileNumber;
    /** WAL files numbered below this are fully reflected in tables and can be ignored. 0 = unchanged. */
    long logNumber;

    VersionEdit add(int level, TableMeta meta) {
        added.add(new Added(level, meta));
        return this;
    }

    VersionEdit remove(int level, long number) {
        removed.add(new Removed(level, number));
        return this;
    }

    byte[] encode() {
        int size = 30;
        for (Added a : added) size += 40 + a.meta().smallest().length + a.meta().largest().length;
        size += removed.size() * 15;
        ByteBuffer out = ByteBuffer.allocate(size);
        Bytes.putVarint(out, nextFileNumber);
        Bytes.putVarint(out, logNumber);
        Bytes.putVarint(out, removed.size());
        for (Removed r : removed) {
            Bytes.putVarint(out, r.level());
            Bytes.putVarint(out, r.number());
        }
        Bytes.putVarint(out, added.size());
        for (Added a : added) {
            TableMeta m = a.meta();
            Bytes.putVarint(out, a.level());
            Bytes.putVarint(out, m.number());
            Bytes.putVarint(out, m.size());
            Bytes.putVarint(out, m.entries());
            Bytes.putVarint(out, m.smallest().length);
            out.put(m.smallest());
            Bytes.putVarint(out, m.largest().length);
            out.put(m.largest());
        }
        byte[] result = new byte[out.position()];
        System.arraycopy(out.array(), 0, result, 0, result.length);
        return result;
    }

    static VersionEdit decode(byte[] data) {
        ByteBuffer in = ByteBuffer.wrap(data);
        VersionEdit e = new VersionEdit();
        e.nextFileNumber = Bytes.getVarint(in);
        e.logNumber = Bytes.getVarint(in);
        long removed = Bytes.getVarint(in);
        for (long i = 0; i < removed; i++) e.remove((int) Bytes.getVarint(in), Bytes.getVarint(in));
        long added = Bytes.getVarint(in);
        for (long i = 0; i < added; i++) {
            int level = (int) Bytes.getVarint(in);
            long number = Bytes.getVarint(in);
            long size = Bytes.getVarint(in);
            long entries = Bytes.getVarint(in);
            byte[] smallest = new byte[(int) Bytes.getVarint(in)];
            in.get(smallest);
            byte[] largest = new byte[(int) Bytes.getVarint(in)];
            in.get(largest);
            e.add(level, new TableMeta(number, size, smallest, largest, entries));
        }
        return e;
    }
}
