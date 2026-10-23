package io.github.tendo505.strata;

import io.github.tendo505.strata.sstable.TableMeta;
import io.github.tendo505.strata.util.Bytes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An immutable picture of which SSTables make up the database, per level.
 *
 * <p>Level 0 holds freshly flushed tables, which may overlap; they are kept
 * newest first, because for a key in several of them the newest wins. Every
 * deeper level is a sorted run of non-overlapping tables, so a key lives in at
 * most one table per level and a binary search finds it.
 *
 * <p>Readers pin the version they started with ({@link #refs}), so compaction
 * can replace tables without deleting files that a scan is still reading.
 */
final class Version {
    final List<List<TableMeta>> levels;
    int refs; // guarded by the database's version lock

    private Version(List<List<TableMeta>> levels) {
        this.levels = levels;
    }

    static Version empty() {
        List<List<TableMeta>> levels = new ArrayList<>();
        for (int i = 0; i < Options.LEVELS; i++) levels.add(List.of());
        return new Version(List.copyOf(levels));
    }

    Version apply(VersionEdit edit) {
        List<List<TableMeta>> next = new ArrayList<>();
        for (int level = 0; level < Options.LEVELS; level++) {
            final int l = level;
            List<TableMeta> files = new ArrayList<>(levels.get(level));
            files.removeIf(f -> edit.removed.stream().anyMatch(r -> r.level() == l && r.number() == f.number()));
            for (VersionEdit.Added a : edit.added) if (a.level() == level) files.add(a.meta());
            if (level == 0) files.sort(Comparator.comparingLong(TableMeta::number).reversed());
            else files.sort((x, y) -> Bytes.compare(x.smallest(), y.smallest()));
            next.add(List.copyOf(files));
        }
        return new Version(List.copyOf(next));
    }

    long levelBytes(int level) {
        long sum = 0;
        for (TableMeta f : levels.get(level)) sum += f.size();
        return sum;
    }

    /** Tables in {@code level} whose key range overlaps [from, to]. */
    List<TableMeta> overlapping(int level, byte[] from, byte[] to) {
        List<TableMeta> out = new ArrayList<>();
        for (TableMeta f : levels.get(level)) if (f.overlaps(from, to)) out.add(f);
        return out;
    }

    /** The one table in a sorted level (1+) that could hold {@code key}, or null. */
    TableMeta find(int level, byte[] key) {
        List<TableMeta> files = levels.get(level);
        int lo = 0, hi = files.size() - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            TableMeta f = files.get(mid);
            if (Bytes.compare(f.largest(), key) < 0) lo = mid + 1;
            else if (Bytes.compare(f.smallest(), key) > 0) hi = mid - 1;
            else return f;
        }
        return null;
    }

    Set<Long> fileNumbers() {
        Set<Long> out = new HashSet<>();
        for (List<TableMeta> level : levels) for (TableMeta f : level) out.add(f.number());
        return out;
    }

    /** Checks the structural invariants; used by tests and by {@code strata check}. */
    void verify() {
        for (int level = 1; level < Options.LEVELS; level++) {
            List<TableMeta> files = levels.get(level);
            for (int i = 1; i < files.size(); i++) {
                if (Bytes.compare(files.get(i - 1).largest(), files.get(i).smallest()) >= 0) {
                    throw new IllegalStateException("level " + level + " has overlapping tables " + files.get(i - 1) + " and " + files.get(i));
                }
            }
        }
    }
}
