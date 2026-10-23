package io.github.tendo505.strata.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Comparator;
import java.util.zip.CRC32C;

/** Byte-array helpers: unsigned ordering, varints and checksums. */
public final class Bytes {
    private Bytes() {}

    /** Keys sort as unsigned bytes, so "a" < "b" and 0x7f < 0x80, the same as LevelDB and RocksDB. */
    public static final Comparator<byte[]> ORDER = Arrays::compareUnsigned;

    public static int compare(byte[] a, byte[] b) {
        return Arrays.compareUnsigned(a, b);
    }

    public static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    public static String string(byte[] b) {
        return b == null ? null : new String(b, StandardCharsets.UTF_8);
    }

    public static int crc(byte[] data, int offset, int length) {
        CRC32C crc = new CRC32C();
        crc.update(data, offset, length);
        return (int) crc.getValue();
    }

    /** Unsigned LEB128: 7 bits per byte, high bit set on every byte but the last. */
    public static void putVarint(ByteBuffer out, long value) {
        while ((value & ~0x7FL) != 0) {
            out.put((byte) ((value & 0x7F) | 0x80));
            value >>>= 7;
        }
        out.put((byte) value);
    }

    public static long getVarint(ByteBuffer in) {
        long result = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            byte b = in.get();
            result |= (long) (b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
        }
        throw new IllegalStateException("corrupt varint");
    }

    public static int varintSize(long value) {
        int n = 1;
        while ((value & ~0x7FL) != 0) {
            value >>>= 7;
            n++;
        }
        return n;
    }

    /** A 64-bit hash (MurmurHash3's finaliser over 8-byte chunks), for Bloom filters. */
    public static long hash(byte[] key) {
        long h = 0x9E3779B97F4A7C15L ^ key.length;
        int i = 0;
        for (; i + 8 <= key.length; i += 8) {
            long k = 0;
            for (int j = 0; j < 8; j++) k |= (key[i + j] & 0xFFL) << (8 * j);
            h = mix(h ^ mix(k));
        }
        long k = 0;
        for (int j = 0; i + j < key.length; j++) k |= (key[i + j] & 0xFFL) << (8 * j);
        return mix(h ^ mix(k));
    }

    private static long mix(long z) {
        z = (z ^ (z >>> 33)) * 0xFF51AFD7ED558CCDL;
        z = (z ^ (z >>> 33)) * 0xC4CEB9FE1A85EC53L;
        return z ^ (z >>> 33);
    }
}
