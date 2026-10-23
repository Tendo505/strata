package io.github.tendo505.strata.sstable;

import io.github.tendo505.strata.Cursor;
import io.github.tendo505.strata.Entry;
import io.github.tendo505.strata.util.Bytes;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Reads an SSTable written by {@link TableWriter}. Safe for concurrent use. */
public final class TableReader implements Closeable {

    /** A decoded data block. A {@code null} value is a tombstone. */
    record Block(byte[][] keys, byte[][] values, int bytes) {}

    private final long number;
    private final FileChannel channel;
    private final byte[][] lastKeys;
    private final long[] offsets;
    private final int[] sizes;
    private final BloomFilter bloom;
    private final BlockCache cache;
    private final long entries;

    private TableReader(long number, FileChannel channel, byte[][] lastKeys, long[] offsets, int[] sizes,
                        BloomFilter bloom, BlockCache cache, long entries) {
        this.number = number;
        this.channel = channel;
        this.lastKeys = lastKeys;
        this.offsets = offsets;
        this.sizes = sizes;
        this.bloom = bloom;
        this.cache = cache;
        this.entries = entries;
    }

    public static TableReader open(Path path, long number, BlockCache cache) throws IOException {
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            long size = channel.size();
            if (size < TableWriter.FOOTER_BYTES) throw new IOException(path + ": too short to be an SSTable");
            ByteBuffer footer = read(channel, size - TableWriter.FOOTER_BYTES, TableWriter.FOOTER_BYTES);
            long indexOffset = footer.getLong();
            int indexSize = footer.getInt();
            long bloomOffset = footer.getLong();
            int bloomSize = footer.getInt();
            long entries = footer.getLong();
            if (footer.getLong() != TableWriter.MAGIC) throw new IOException(path + ": not an SSTable (bad magic)");

            BloomFilter bloom = BloomFilter.decode(verified(channel, bloomOffset, bloomSize, path));
            ByteBuffer idx = ByteBuffer.wrap(verified(channel, indexOffset, indexSize, path));
            int blocks = (int) Bytes.getVarint(idx);
            byte[][] lastKeys = new byte[blocks][];
            long[] offsets = new long[blocks];
            int[] sizes = new int[blocks];
            for (int i = 0; i < blocks; i++) {
                lastKeys[i] = new byte[(int) Bytes.getVarint(idx)];
                idx.get(lastKeys[i]);
                offsets[i] = Bytes.getVarint(idx);
                sizes[i] = (int) Bytes.getVarint(idx);
            }
            return new TableReader(number, channel, lastKeys, offsets, sizes, bloom, cache, entries);
        } catch (IOException | RuntimeException e) {
            channel.close();
            throw e;
        }
    }

    public long number() {
        return number;
    }

    public long entries() {
        return entries;
    }

    /** The Bloom filter's answer: false means the key is certainly not in this table. */
    public boolean mightContain(byte[] key) {
        return bloom.mightContain(key);
    }

    /**
     * Looks a key up. Returns {@code null} if the table doesn't have it, or an
     * entry whose value is {@code null} if the table says it was deleted.
     */
    public Entry get(byte[] key) {
        if (!bloom.mightContain(key)) return null;
        int b = blockFor(key);
        if (b < 0) return null;
        Block block = block(b);
        int lo = 0, hi = block.keys().length - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            int c = Bytes.compare(block.keys()[mid], key);
            if (c == 0) return new Entry(block.keys()[mid], block.values()[mid]);
            if (c < 0) lo = mid + 1;
            else hi = mid - 1;
        }
        return null;
    }

    /** Entries from {@code from} (inclusive; null = the start), tombstones included. */
    public Cursor cursor(byte[] from) {
        return new Cursor() {
            private int blockIndex = from == null ? 0 : blockFor(from);
            private Block block;
            private int pos;

            {
                if (blockIndex >= 0 && blockIndex < lastKeys.length) {
                    block = block(blockIndex);
                    if (from != null) while (pos < block.keys().length && Bytes.compare(block.keys()[pos], from) < 0) pos++;
                    skipEmpty();
                } else {
                    blockIndex = lastKeys.length;
                }
            }

            private void skipEmpty() {
                while (block != null && pos >= block.keys().length) {
                    blockIndex++;
                    block = blockIndex < lastKeys.length ? block(blockIndex) : null;
                    pos = 0;
                }
            }

            @Override
            public boolean valid() {
                return block != null;
            }

            @Override
            public byte[] key() {
                return block.keys()[pos];
            }

            @Override
            public byte[] value() {
                return block.values()[pos];
            }

            @Override
            public void next() {
                pos++;
                skipEmpty();
            }
        };
    }

    /** The first block whose last key is at least {@code key}, or -1 if the key is past the end. */
    private int blockFor(byte[] key) {
        int lo = 0, hi = lastKeys.length - 1, found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (Bytes.compare(lastKeys[mid], key) >= 0) {
                found = mid;
                hi = mid - 1;
            } else {
                lo = mid + 1;
            }
        }
        return found;
    }

    private Block block(int i) {
        BlockCache.Key key = new BlockCache.Key(number, i);
        Block cached = cache.get(key);
        if (cached != null) return cached;
        try {
            byte[] data = verified(channel, offsets[i], sizes[i], null);
            ByteBuffer in = ByteBuffer.wrap(data);
            List<byte[]> keys = new ArrayList<>();
            List<byte[]> values = new ArrayList<>();
            while (in.hasRemaining()) {
                byte[] k = new byte[(int) Bytes.getVarint(in)];
                in.get(k);
                byte[] v = null;
                if (in.get() == 1) {
                    v = new byte[(int) Bytes.getVarint(in)];
                    in.get(v);
                }
                keys.add(k);
                values.add(v);
            }
            Block block = new Block(keys.toArray(byte[][]::new), values.toArray(byte[][]::new), data.length + keys.size() * 32);
            cache.put(key, block);
            return block;
        } catch (IOException e) {
            throw new UncheckedIOException("table " + number + ", block " + i, e);
        }
    }

    private static ByteBuffer read(FileChannel channel, long offset, int length) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(length);
        while (buf.hasRemaining()) {
            if (channel.read(buf, offset + buf.position()) < 0) throw new IOException("unexpected end of file");
        }
        return buf.flip();
    }

    /** Reads a checksummed section and returns its payload, or fails loudly if the bytes on disk are damaged. */
    private static byte[] verified(FileChannel channel, long offset, int size, Path path) throws IOException {
        ByteBuffer buf = read(channel, offset, size);
        int length = size - 4;
        byte[] data = new byte[length];
        buf.get(data);
        if (Bytes.crc(data, 0, length) != buf.getInt()) {
            throw new IOException((path == null ? "SSTable" : path.toString()) + ": checksum mismatch at offset " + offset + " (corrupt file)");
        }
        return data;
    }

    @Override
    public void close() throws IOException {
        cache.evictTable(number);
        channel.close();
    }
}
