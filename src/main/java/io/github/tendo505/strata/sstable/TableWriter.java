package io.github.tendo505.strata.sstable;

import io.github.tendo505.strata.util.Bytes;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes an SSTable: an immutable file of sorted entries.
 *
 * <pre>
 * [data block]...[data block] [bloom filter] [index block] [footer]
 *
 * data block   entries (keyLen, key, flag, valueLen, value), then crc32c
 * bloom filter k, bit array, then crc32c
 * index block  per data block: its last key, offset and size; then crc32c
 * footer       indexOffset:8 indexSize:4 bloomOffset:8 bloomSize:4 entries:8 magic:8
 * </pre>
 *
 * A lookup reads the footer, index and filter once (when the table is
 * opened), then at most one data block.
 */
public final class TableWriter implements Closeable {
    static final long MAGIC = 0x5354524154413031L; // "STRATA01"
    static final int FOOTER_BYTES = 40;

    private record IndexEntry(byte[] lastKey, long offset, int size) {}

    private final FileChannel channel;
    private final int blockBytes;
    private final int bloomBitsPerKey;
    private ByteBuffer block;
    private final List<IndexEntry> index = new ArrayList<>();
    private final List<Long> hashes = new ArrayList<>();
    private byte[] smallest;
    private byte[] last;
    private long offset;
    private long entries;
    private boolean finished;

    public TableWriter(Path path, int blockBytes, int bloomBitsPerKey) throws IOException {
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        this.blockBytes = blockBytes;
        this.bloomBitsPerKey = bloomBitsPerKey;
        this.block = ByteBuffer.allocate(blockBytes * 2);
    }

    /** Adds an entry; keys must be strictly increasing. A {@code null} value writes a tombstone. */
    public void add(byte[] key, byte[] value) throws IOException {
        if (last != null && Bytes.compare(key, last) <= 0) throw new IllegalArgumentException("keys must be added in increasing order");
        int need = 1 + 5 + key.length + (value == null ? 0 : 5 + value.length);
        if (block.remaining() < need) grow(need);
        Bytes.putVarint(block, key.length);
        block.put(key);
        block.put((byte) (value == null ? 0 : 1));
        if (value != null) {
            Bytes.putVarint(block, value.length);
            block.put(value);
        }
        if (smallest == null) smallest = key;
        last = key;
        hashes.add(Bytes.hash(key));
        entries++;
        if (block.position() >= blockBytes) flushBlock();
    }

    /** Bytes written so far plus the block being built: used to split compaction output into files. */
    public long estimatedSize() {
        return offset + block.position();
    }

    public long entries() {
        return entries;
    }

    private void grow(int need) {
        ByteBuffer bigger = ByteBuffer.allocate(Math.max(block.capacity() * 2, block.position() + need));
        block.flip();
        bigger.put(block);
        block = bigger;
    }

    private void flushBlock() throws IOException {
        if (block.position() == 0) return;
        int size = writeChecksummed(block.array(), block.position());
        index.add(new IndexEntry(last, offset, size));
        offset += size;
        block.clear();
    }

    private int writeChecksummed(byte[] data, int length) throws IOException {
        ByteBuffer out = ByteBuffer.allocate(length + 4);
        out.put(data, 0, length).putInt(Bytes.crc(data, 0, length)).flip();
        while (out.hasRemaining()) channel.write(out);
        return length + 4;
    }

    /** Writes the filter, index and footer and syncs the file to disk. */
    public TableMeta finish(long number) throws IOException {
        flushBlock();
        byte[] bloom = BloomFilter.build(hashes, bloomBitsPerKey).encode();
        long bloomOffset = offset;
        int bloomSize = writeChecksummed(bloom, bloom.length);
        offset += bloomSize;

        int indexBytes = 5;
        for (IndexEntry e : index) indexBytes += 5 + e.lastKey().length + 10 + 5;
        ByteBuffer idx = ByteBuffer.allocate(indexBytes);
        Bytes.putVarint(idx, index.size());
        for (IndexEntry e : index) {
            Bytes.putVarint(idx, e.lastKey().length);
            idx.put(e.lastKey());
            Bytes.putVarint(idx, e.offset());
            Bytes.putVarint(idx, e.size());
        }
        long indexOffset = offset;
        int indexSize = writeChecksummed(idx.array(), idx.position());
        offset += indexSize;

        ByteBuffer footer = ByteBuffer.allocate(FOOTER_BYTES);
        footer.putLong(indexOffset).putInt(indexSize).putLong(bloomOffset).putInt(bloomSize).putLong(entries).putLong(MAGIC).flip();
        while (footer.hasRemaining()) channel.write(footer);
        offset += FOOTER_BYTES;
        channel.force(true);
        channel.close();
        finished = true;
        return new TableMeta(number, offset, smallest, last, entries);
    }

    @Override
    public void close() throws IOException {
        if (!finished) channel.close();
    }
}
