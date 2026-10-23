package io.github.tendo505.strata;

/**
 * Tuning knobs. The defaults suit a laptop; tests shrink the sizes so a few
 * thousand writes exercise flushes and every level of compaction.
 *
 * @param memtableBytes      flush the memtable to an SSTable once it holds about this much
 * @param syncWrites         fsync the WAL on every write: survives power loss, not just process crashes
 * @param blockBytes         target size of an SSTable data block
 * @param bloomBitsPerKey    Bloom filter size; 10 bits per key gives about 1% false positives
 * @param l0CompactionTrigger compact level 0 into level 1 once it has this many files
 * @param l0StopTrigger      stall writes once level 0 has this many files, until compaction catches up
 * @param level1Bytes        size limit of level 1; each deeper level may be {@code levelMultiplier} times larger
 * @param levelMultiplier    growth factor between levels
 * @param targetFileBytes    split compaction output into files of about this size
 * @param blockCacheBytes    memory for recently read data blocks
 */
public record Options(
        long memtableBytes,
        boolean syncWrites,
        int blockBytes,
        int bloomBitsPerKey,
        int l0CompactionTrigger,
        int l0StopTrigger,
        long level1Bytes,
        int levelMultiplier,
        long targetFileBytes,
        long blockCacheBytes) {

    public static final int LEVELS = 7;

    public static Options defaults() {
        return new Options(4 << 20, false, 4096, 10, 4, 12, 10L << 20, 10, 2 << 20, 32L << 20);
    }

    /** Tiny sizes so that tests reach deep levels quickly. */
    public static Options tiny() {
        return new Options(8 << 10, false, 512, 10, 4, 12, 16 << 10, 2, 8 << 10, 256 << 10);
    }

    public Options withSyncWrites(boolean sync) {
        return new Options(memtableBytes, sync, blockBytes, bloomBitsPerKey, l0CompactionTrigger, l0StopTrigger,
                level1Bytes, levelMultiplier, targetFileBytes, blockCacheBytes);
    }

    public Options withMemtableBytes(long bytes) {
        return new Options(bytes, syncWrites, blockBytes, bloomBitsPerKey, l0CompactionTrigger, l0StopTrigger,
                level1Bytes, levelMultiplier, targetFileBytes, blockCacheBytes);
    }

    public long maxBytesForLevel(int level) {
        long bytes = level1Bytes;
        for (int i = 1; i < level; i++) bytes *= levelMultiplier;
        return bytes;
    }
}
