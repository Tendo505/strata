package io.github.tendo505.strata.sstable;

import io.github.tendo505.strata.util.Bytes;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Answers "is this key possibly in the table?" with no false negatives and,
 * at 10 bits per key, about 1% false positives, so most lookups for absent
 * keys skip the table without reading a block. Uses double hashing
 * (Kirsch and Mitzenmacher) to derive k bit positions from one 64-bit hash.
 */
public final class BloomFilter {
    private final long[] bits;
    private final int k;

    private BloomFilter(long[] bits, int k) {
        this.bits = bits;
        this.k = k;
    }

    public static BloomFilter build(List<Long> hashes, int bitsPerKey) {
        int k = Math.clamp(Math.round(bitsPerKey * 0.69f), 1, 30); // ln 2 * bits per key minimises false positives
        long nbits = Math.max(64, (long) hashes.size() * bitsPerKey);
        long[] bits = new long[(int) ((nbits + 63) / 64)];
        BloomFilter f = new BloomFilter(bits, k);
        for (long h : hashes) f.add(h);
        return f;
    }

    private void add(long hash) {
        long n = (long) bits.length * 64;
        long delta = Long.rotateRight(hash, 17);
        for (int i = 0; i < k; i++) {
            long bit = Long.remainderUnsigned(hash, n);
            bits[(int) (bit >>> 6)] |= 1L << (bit & 63);
            hash += delta;
        }
    }

    public boolean mightContain(byte[] key) {
        long hash = Bytes.hash(key);
        long n = (long) bits.length * 64;
        long delta = Long.rotateRight(hash, 17);
        for (int i = 0; i < k; i++) {
            long bit = Long.remainderUnsigned(hash, n);
            if ((bits[(int) (bit >>> 6)] & (1L << (bit & 63))) == 0) return false;
            hash += delta;
        }
        return true;
    }

    public byte[] encode() {
        ByteBuffer out = ByteBuffer.allocate(1 + bits.length * 8);
        out.put((byte) k);
        for (long word : bits) out.putLong(word);
        return out.array();
    }

    public static BloomFilter decode(byte[] data) {
        ByteBuffer in = ByteBuffer.wrap(data);
        int k = in.get();
        long[] bits = new long[in.remaining() / 8];
        for (int i = 0; i < bits.length; i++) bits[i] = in.getLong();
        return new BloomFilter(bits, k);
    }
}
