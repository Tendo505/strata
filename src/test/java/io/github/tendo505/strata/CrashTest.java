package io.github.tendo505.strata;

import static io.github.tendo505.strata.util.Bytes.string;
import static io.github.tendo505.strata.util.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Kills a writing process for real, at random moments (often mid-flush or
 * mid-compaction, since the options are tiny), then checks that reopening
 * finds every write the process had acknowledged, and nothing it hadn't
 * started.
 */
class CrashTest {
    @TempDir
    Path dir;

    /** The workload: deterministic and idempotent, so the expected state after n operations is easy to compute. */
    static final class Workload {
        static String key(int i) {
            return String.format("k%05d", Math.floorMod(i * 7919, 4000));
        }

        static void apply(Strata db, int i) throws IOException {
            if (i % 5 == 4) {
                db.delete(utf8(key(i - 2)));
            } else if (i % 7 == 6) {
                db.write(new WriteBatch().put(utf8(key(i)), utf8("batch" + i)).put(utf8(key(i + 1)), utf8("batch" + i)));
            } else {
                db.put(utf8(key(i)), utf8("value" + i + "-" + "x".repeat(i % 50)));
            }
        }

        static TreeMap<String, String> expected(int lastOp) {
            TreeMap<String, String> m = new TreeMap<>();
            for (int i = 0; i <= lastOp; i++) {
                if (i % 5 == 4) {
                    m.remove(key(i - 2));
                } else if (i % 7 == 6) {
                    m.put(key(i), "batch" + i);
                    m.put(key(i + 1), "batch" + i);
                } else {
                    m.put(key(i), "value" + i + "-" + "x".repeat(i % 50));
                }
            }
            return m;
        }
    }

    /** The child process: writes from operation {@code start} on, printing each number once its write returned. */
    public static void main(String[] args) throws IOException {
        Path dir = Path.of(args[0]);
        boolean sync = Boolean.parseBoolean(args[1]);
        int start = Integer.parseInt(args[2]);
        try (Strata db = Strata.open(dir, Options.tiny().withSyncWrites(sync))) {
            for (int i = start; ; i++) {
                Workload.apply(db, i);
                System.out.println(i);
                System.out.flush();
            }
        }
    }

    private static TreeMap<String, String> contents(Strata db) throws IOException {
        TreeMap<String, String> m = new TreeMap<>();
        try (Strata.Scan s = db.scan(null, null)) {
            while (s.hasNext()) {
                Entry e = s.next();
                m.put(string(e.key()), string(e.value()));
            }
        }
        return m;
    }

    @Test
    void acknowledgedWritesSurviveTheProcessBeingKilled() throws Exception {
        Random rnd = new Random(11);
        int next = 0;
        for (int round = 0; round < 6; round++) {
            boolean sync = round % 2 == 1;
            String java = Path.of(System.getProperty("java.home"), "bin", "java").toString();
            Process child = new ProcessBuilder(java, "-cp", System.getProperty("java.class.path"),
                    CrashTest.class.getName(), dir.toString(), String.valueOf(sync), String.valueOf(next))
                    .redirectErrorStream(true).start();
            AtomicInteger acked = new AtomicInteger(next - 1);
            int target = next + 300 + rnd.nextInt(sync ? 400 : 4000);
            StringBuilder unexpected = new StringBuilder();
            Thread reader = new Thread(() -> {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(child.getInputStream(), StandardCharsets.UTF_8))) {
                    for (String line; (line = in.readLine()) != null; ) {
                        try {
                            acked.set(Integer.parseInt(line.trim()));
                        } catch (NumberFormatException e) {
                            unexpected.append(line).append('\n');
                        }
                    }
                } catch (IOException ignored) {
                    // the child was killed
                }
            });
            reader.start();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (acked.get() < target && child.isAlive() && System.nanoTime() < deadline) Thread.sleep(1);
            child.destroyForcibly(); // SIGKILL / TerminateProcess: no shutdown hooks, no close()
            child.waitFor(30, TimeUnit.SECONDS);
            reader.join(5000);
            assertTrue(unexpected.isEmpty(), "child printed: " + unexpected);
            int last = acked.get();
            assertTrue(last >= target, "child got to " + last + " of " + target);

            try (Strata db = Strata.open(dir, Options.tiny())) {
                TreeMap<String, String> actual = contents(db);
                // The write in flight when the process died may or may not have made it; nothing later can have.
                boolean exactlyAcked = actual.equals(Workload.expected(last));
                boolean plusInFlight = actual.equals(Workload.expected(last + 1));
                assertTrue(exactlyAcked || plusInFlight, "round " + round + ": state after recovery is not the state after op " + last);
                db.verify();
                System.out.printf("round %d (%s): killed after %d acknowledged writes; recovered %s%n", round,
                        sync ? "fsync each write" : "no fsync", last + 1, exactlyAcked ? "exactly those" : "those plus the in-flight write");
                next = (plusInFlight ? last + 2 : last + 1);
            }
        }
    }
}
