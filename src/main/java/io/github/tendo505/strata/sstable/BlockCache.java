package io.github.tendo505.strata.sstable;

import java.util.LinkedHashMap;
import java.util.Map;

/** A small LRU cache of decoded data blocks, shared by all tables, bounded by an estimate of their size. */
public final class BlockCache {
    record Key(long table, int block) {}

    private final long capacity;
    private long used;
    private long hits;
    private long misses;
    private final LinkedHashMap<Key, TableReader.Block> map = new LinkedHashMap<>(256, 0.75f, true);

    public BlockCache(long capacityBytes) {
        this.capacity = capacityBytes;
    }

    synchronized TableReader.Block get(Key key) {
        TableReader.Block b = map.get(key);
        if (b == null) misses++;
        else hits++;
        return b;
    }

    synchronized void put(Key key, TableReader.Block block) {
        if (capacity <= 0 || map.containsKey(key)) return;
        map.put(key, block);
        used += block.bytes();
        var it = map.entrySet().iterator();
        while (used > capacity && it.hasNext()) {
            Map.Entry<Key, TableReader.Block> eldest = it.next();
            used -= eldest.getValue().bytes();
            it.remove();
        }
    }

    synchronized void evictTable(long table) {
        var it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Key, TableReader.Block> e = it.next();
            if (e.getKey().table() == table) {
                used -= e.getValue().bytes();
                it.remove();
            }
        }
    }

    public synchronized double hitRate() {
        long total = hits + misses;
        return total == 0 ? 0 : (double) hits / total;
    }
}
