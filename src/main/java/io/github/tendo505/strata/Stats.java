package io.github.tendo505.strata;

import java.util.List;

/**
 * A snapshot of the engine's shape and work done.
 *
 * <p>Write amplification is bytes written to SSTables (by flushes and
 * compactions) divided by bytes the user wrote: the price an LSM tree pays
 * for turning random writes into sequential ones.
 */
public record Stats(
        List<Integer> filesPerLevel,
        List<Long> bytesPerLevel,
        long memtableBytes,
        long userBytesWritten,
        long tableBytesWritten,
        long flushes,
        long compactions,
        long trivialMoves,
        long writeStalls,
        double blockCacheHitRate) {

    public double writeAmplification() {
        return userBytesWritten == 0 ? 0 : (double) tableBytesWritten / userBytesWritten;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int level = 0; level < filesPerLevel.size(); level++) {
            if (filesPerLevel.get(level) == 0) continue;
            sb.append(String.format("  L%d: %4d files, %8.2f MiB%n", level, filesPerLevel.get(level), bytesPerLevel.get(level) / 1048576.0));
        }
        sb.append(String.format("  %d flushes, %d compactions (%d trivial moves), %d write stalls, write amplification %.2f, block cache hit rate %.0f%%",
                flushes, compactions, trivialMoves, writeStalls, writeAmplification(), blockCacheHitRate * 100));
        return sb.toString();
    }
}
