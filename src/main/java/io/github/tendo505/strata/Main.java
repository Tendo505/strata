package io.github.tendo505.strata;

import static io.github.tendo505.strata.util.Bytes.string;
import static io.github.tendo505.strata.util.Bytes.utf8;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;
import java.util.Random;
import java.util.stream.Stream;

/** Command line: a small shell over a database directory, and a db_bench-style benchmark. */
public final class Main {
    private static final String USAGE = """
            strata: an LSM-tree key-value store

              strata put <dir> <key> <value>
              strata get <dir> <key>
              strata delete <dir> <key>
              strata scan <dir> [from] [to]
              strata stats <dir>
              strata compact <dir>
              strata bench [keys] [--sync]     LevelDB-style benchmark in a temporary directory
                                               (16-byte keys, 100-byte values; default 1,000,000 keys)""";

    private Main() {}

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.out.println(USAGE);
            return;
        }
        switch (args[0]) {
            case "bench" -> bench(args);
            case "put", "get", "delete", "scan", "stats", "compact" -> shell(args);
            default -> {
                System.out.println(USAGE);
                System.exit(2);
            }
        }
    }

    private static void shell(String[] args) throws IOException {
        if (args.length < 2) throw new IllegalArgumentException("missing <dir>");
        try (Strata db = Strata.open(Path.of(args[1]))) {
            switch (args[0]) {
                case "put" -> db.put(utf8(args[2]), utf8(args[3]));
                case "get" -> {
                    byte[] v = db.get(utf8(args[2]));
                    if (v == null) {
                        System.err.println("(not found)");
                        System.exit(1);
                    }
                    System.out.println(string(v));
                }
                case "delete" -> db.delete(utf8(args[2]));
                case "scan" -> {
                    try (Strata.Scan s = db.scan(args.length > 2 ? utf8(args[2]) : null, args.length > 3 ? utf8(args[3]) : null)) {
                        while (s.hasNext()) {
                            Entry e = s.next();
                            System.out.println(string(e.key()) + "\t" + string(e.value()));
                        }
                    }
                }
                case "stats" -> System.out.println(db.stats());
                case "compact" -> {
                    db.compactAll();
                    System.out.println(db.stats());
                }
                default -> throw new IllegalStateException();
            }
        }
    }

    // ---------------- benchmark ----------------

    private static final int VALUE = 100;

    /** A 16-digit zero-padded decimal key, built without String.format so key generation doesn't dominate the timings. */
    private static byte[] key(long k) {
        byte[] out = new byte[16];
        for (int i = 15; i >= 0; i--) {
            out[i] = (byte) ('0' + k % 10);
            k /= 10;
        }
        return out;
    }

    private static byte[] missingKey(long k) {
        return Arrays.copyOf(key(k), 17); // same prefix plus one more byte: inside the key range, but never written
    }

    private static void bench(String[] args) throws IOException {
        int n = args.length > 1 && !args[1].startsWith("--") ? Integer.parseInt(args[1].replace("_", "")) : 1_000_000;
        boolean sync = Arrays.asList(args).contains("--sync");
        Path dir = Files.createTempDirectory("strata-bench");
        Options options = Options.defaults().withSyncWrites(sync);
        Random rnd = new Random(301);
        byte[] value = new byte[VALUE];
        System.out.printf(Locale.ROOT, "Strata benchmark: %,d keys, 16-byte keys, %d-byte values, %s, Java %s, %d cores%n%n",
                n, VALUE, sync ? "fsync every write" : "no fsync (process-crash safe)", System.getProperty("java.version"),
                Runtime.getRuntime().availableProcessors());
        System.out.println("| workload | ops/s | MB/s | notes |");
        System.out.println("| --- | ---: | ---: | --- |");
        try {
            int writes = sync ? Math.min(n, 20_000) : n;
            try (Strata db = Strata.open(dir, options)) {
                long t = System.nanoTime();
                for (int i = 0; i < writes; i++) {
                    rnd.nextBytes(value);
                    db.put(key(i), value);
                }
                db.flush();
                report("fillseq", writes, System.nanoTime() - t, "sequential keys");
            }
            deleteDir(dir);

            int[] order = shuffled(n, rnd);
            try (Strata db = Strata.open(dir, options)) {
                long t = System.nanoTime();
                for (int i = 0; i < writes; i++) {
                    rnd.nextBytes(value);
                    db.put(key(order[i]), value);
                }
                db.flush();
                db.awaitBackground();
                report("fillrandom", writes, System.nanoTime() - t, "random order; includes compaction to a steady shape");
                System.out.println();
                System.out.println("After fillrandom:");
                System.out.println(db.stats());
                System.out.println();

                long[] latencies = new long[Math.min(n, 200_000)];
                long t2 = System.nanoTime();
                for (int i = 0; i < latencies.length; i++) {
                    long s = System.nanoTime();
                    if (db.get(key(rnd.nextInt(writes))) == null) throw new IllegalStateException("missing key");
                    latencies[i] = System.nanoTime() - s;
                }
                report("readrandom", latencies.length, System.nanoTime() - t2, percentiles(latencies));

                long t3 = System.nanoTime();
                for (int i = 0; i < latencies.length; i++) {
                    long s = System.nanoTime();
                    if (db.get(missingKey(rnd.nextInt(writes))) != null) throw new IllegalStateException("phantom key");
                    latencies[i] = System.nanoTime() - s;
                }
                report("readmissing", latencies.length, System.nanoTime() - t3, percentiles(latencies) + "; Bloom filters skip most tables");

                int seeks = 20_000;
                long t4 = System.nanoTime();
                long scanned = 0;
                for (int i = 0; i < seeks; i++) {
                    try (Strata.Scan s = db.scan(key(rnd.nextInt(writes)), null)) {
                        for (int j = 0; j < 100 && s.hasNext(); j++, scanned++) s.next();
                    }
                }
                report("seekrandom + 100 next", seeks, System.nanoTime() - t4, String.format(Locale.ROOT, "%,d entries scanned", scanned));

                long t5 = System.nanoTime();
                for (int i = 0; i < writes; i++) {
                    rnd.nextBytes(value);
                    db.put(key(order[(i * 7) % writes]), value);
                }
                db.flush();
                db.awaitBackground();
                report("overwrite", writes, System.nanoTime() - t5, "every key again, random order");
                System.out.println();
                System.out.println("After overwrite:");
                System.out.println(db.stats());
            }
        } finally {
            deleteDir(dir);
        }
    }

    private static void report(String name, long ops, long nanos, String notes) {
        double seconds = nanos / 1e9;
        double mb = ops * (16.0 + VALUE) / 1e6 / seconds;
        System.out.printf(Locale.ROOT, "| %s | %,.0f | %.1f | %s |%n", name, ops / seconds, mb, notes);
    }

    private static String percentiles(long[] nanos) {
        long[] sorted = nanos.clone();
        Arrays.sort(sorted);
        return String.format(Locale.ROOT, "p50 %.1f us, p99 %.1f us", sorted[sorted.length / 2] / 1e3, sorted[(int) (sorted.length * 0.99)] / 1e3);
    }

    private static int[] shuffled(int n, Random rnd) {
        int[] a = new int[n];
        for (int i = 0; i < n; i++) a[i] = i;
        for (int i = n - 1; i > 0; i--) {
            int j = rnd.nextInt(i + 1);
            int t = a[i];
            a[i] = a[j];
            a[j] = t;
        }
        return a;
    }

    private static void deleteDir(Path dir) throws IOException {
        if (!Files.exists(dir)) return;
        try (Stream<Path> files = Files.walk(dir)) {
            for (Path p : files.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
        }
    }
}
