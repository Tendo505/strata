package io.github.tendo505.strata;

import io.github.tendo505.strata.util.Bytes;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Merges sorted cursors into one, k-way, with a heap. Sources are given
 * newest first; when several hold the same key, the newest one's entry wins
 * and the older ones are skipped. Tombstones pass through: the caller decides
 * whether to hide them (a scan) or keep them (a compaction that isn't at the
 * bottom of the tree).
 */
final class MergingCursor implements Cursor {
    private final Cursor[] sources;
    private final PriorityQueue<Integer> heap;
    private int current = -1;

    MergingCursor(List<Cursor> newestFirst) {
        this.sources = newestFirst.toArray(Cursor[]::new);
        this.heap = new PriorityQueue<>(Math.max(1, sources.length), (a, b) -> {
            int c = Bytes.compare(sources[a].key(), sources[b].key());
            return c != 0 ? c : Integer.compare(a, b); // same key: the newer source (lower index) first
        });
        for (int i = 0; i < sources.length; i++) if (sources[i].valid()) heap.add(i);
        current = heap.isEmpty() ? -1 : heap.peek();
    }

    @Override
    public boolean valid() {
        return current >= 0;
    }

    @Override
    public byte[] key() {
        return sources[current].key();
    }

    @Override
    public byte[] value() {
        return sources[current].value();
    }

    @Override
    public void next() {
        byte[] key = key();
        // Step every source past this key: the newest copy was just returned, the rest are shadowed.
        while (!heap.isEmpty() && Bytes.compare(sources[heap.peek()].key(), key) == 0) {
            int i = heap.poll();
            sources[i].next();
            if (sources[i].valid()) heap.add(i);
        }
        current = heap.isEmpty() ? -1 : heap.peek();
    }
}
