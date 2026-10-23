package io.github.tendo505.strata;

import static io.github.tendo505.strata.util.Bytes.string;
import static io.github.tendo505.strata.util.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.tendo505.strata.util.Bytes;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * One writer bumps per-key counters while readers and scanners run against
 * it and flushes and compactions churn underneath. A reader must never see a
 * counter go backwards, and scans must stay sorted with no duplicates.
 */
class ConcurrencyTest {
    @TempDir
    Path dir;

    private static final int KEYS = 500;

    private static byte[] key(int k) {
        return utf8(String.format("c%04d", k));
    }

    @Test
    void readersNeverSeeTimeGoBackwards() throws Exception {
        try (Strata db = Strata.open(dir, Options.tiny())) {
            AtomicBoolean stop = new AtomicBoolean();
            ConcurrentLinkedQueue<String> problems = new ConcurrentLinkedQueue<>();
            AtomicLong reads = new AtomicLong();
            AtomicLong scans = new AtomicLong();
            long[] written = new long[KEYS];

            Thread writer = new Thread(() -> {
                Random rnd = new Random(1);
                try {
                    while (!stop.get()) {
                        int k = rnd.nextInt(KEYS);
                        db.put(key(k), utf8(Long.toString(++written[k])));
                    }
                } catch (Exception e) {
                    problems.add("writer: " + e);
                }
            });

            List<Thread> readers = new ArrayList<>();
            for (int t = 0; t < 4; t++) {
                final int seed = t;
                readers.add(new Thread(() -> {
                    Random rnd = new Random(100 + seed);
                    long[] seen = new long[KEYS];
                    try {
                        while (!stop.get()) {
                            int k = rnd.nextInt(KEYS);
                            byte[] v = db.get(key(k));
                            long value = v == null ? 0 : Long.parseLong(string(v));
                            if (value < seen[k]) problems.add("key " + k + " went from " + seen[k] + " back to " + value);
                            seen[k] = value;
                            reads.incrementAndGet();
                        }
                    } catch (Exception e) {
                        problems.add("reader: " + e);
                    }
                }));
            }
            for (int t = 0; t < 2; t++) {
                readers.add(new Thread(() -> {
                    try {
                        while (!stop.get()) {
                            byte[] previous = null;
                            try (Strata.Scan s = db.scan(null, null)) {
                                while (s.hasNext()) {
                                    byte[] k = s.next().key();
                                    if (previous != null && Bytes.compare(previous, k) >= 0) problems.add("scan out of order at " + string(k));
                                    previous = k;
                                }
                            }
                            scans.incrementAndGet();
                        }
                    } catch (Exception e) {
                        problems.add("scanner: " + e);
                    }
                }));
            }

            writer.start();
            readers.forEach(Thread::start);
            Thread.sleep(3000);
            stop.set(true);
            writer.join();
            for (Thread r : readers) r.join();

            assertTrue(problems.isEmpty(), () -> String.join("\n", problems.stream().limit(5).toList()));
            db.awaitBackground();
            db.verify();
            for (int k = 0; k < KEYS; k++) {
                assertEquals(written[k] == 0 ? null : Long.toString(written[k]), string(db.get(key(k))));
            }
            Stats stats = db.stats();
            System.out.printf("%,d reads and %,d full scans alongside %,d writes, %d flushes and %d compactions%n",
                    reads.get(), scans.get(), Arrays.stream(written).sum(), stats.flushes(), stats.compactions());
            assertTrue(stats.compactions() > 0);
        }
    }
}
