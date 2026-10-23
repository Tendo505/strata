package io.github.tendo505.strata;

import static io.github.tendo505.strata.util.Bytes.string;
import static io.github.tendo505.strata.util.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Model-based testing: random operations go to Strata and to a TreeMap, and
 * every read must agree. Tiny memtables and level sizes mean a few thousand
 * operations go through dozens of flushes, compactions and reopens.
 */
class ModelTest {
    @TempDir
    Path dir;

    private static String key(int k) {
        return String.format("key%05d", k);
    }

    private static List<String> scan(Strata db, String from, String to) throws IOException {
        List<String> out = new ArrayList<>();
        try (Strata.Scan s = db.scan(from == null ? null : utf8(from), to == null ? null : utf8(to))) {
            while (s.hasNext()) {
                Entry e = s.next();
                out.add(string(e.key()) + "=" + string(e.value()));
            }
        }
        return out;
    }

    private static List<String> scan(TreeMap<String, String> model, String from, String to) {
        Map<String, String> range = from == null && to == null ? model
                : from == null ? model.headMap(to) : to == null ? model.tailMap(from) : model.subMap(from, to);
        List<String> out = new ArrayList<>();
        range.forEach((k, v) -> out.add(k + "=" + v));
        return out;
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, 3, 4, 5, 6, 7, 8})
    void behavesLikeASortedMap(long seed) throws IOException {
        Random rnd = new Random(seed);
        TreeMap<String, String> model = new TreeMap<>();
        Options options = Options.tiny();
        Strata db = Strata.open(dir, options);
        int keySpace = 1500;
        int reopens = 0;
        long compactions = 0;
        int deepestLevel = 0;
        try {
            for (int op = 0; op < 12_000; op++) {
                int r = rnd.nextInt(1000);
                String k = key(rnd.nextInt(keySpace));
                if (r < 450) {
                    String v = "v" + op + "-" + "x".repeat(rnd.nextInt(60));
                    db.put(utf8(k), utf8(v));
                    model.put(k, v);
                } else if (r < 600) {
                    db.delete(utf8(k));
                    model.remove(k);
                } else if (r < 850) {
                    assertEquals(model.get(k), string(db.get(utf8(k))), "get " + k + " at op " + op);
                } else if (r < 900) {
                    WriteBatch batch = new WriteBatch();
                    for (int i = 0; i < 1 + rnd.nextInt(8); i++) {
                        String bk = key(rnd.nextInt(keySpace));
                        if (rnd.nextInt(4) == 0) {
                            batch.delete(utf8(bk));
                            model.remove(bk);
                        } else {
                            batch.put(utf8(bk), utf8("b" + op + "." + i));
                            model.put(bk, "b" + op + "." + i);
                        }
                    }
                    db.write(batch);
                } else if (r < 960) {
                    int a = rnd.nextInt(keySpace), b = a + rnd.nextInt(200);
                    assertEquals(scan(model, key(a), key(b)), scan(db, key(a), key(b)), "scan at op " + op);
                } else if (r < 962) {
                    compactions += db.stats().compactions();
                    db.close();
                    db = Strata.open(dir, options);
                    reopens++;
                } else if (r < 975) {
                    db.flush();
                } else if (r < 976) {
                    db.compactAll();
                    db.verify();
                } else {
                    db.awaitBackground();
                    db.verify();
                    List<Integer> files = db.stats().filesPerLevel();
                    for (int level = 0; level < files.size(); level++) if (files.get(level) > 0) deepestLevel = Math.max(deepestLevel, level);
                }
            }
            assertEquals(scan(model, null, null), scan(db, null, null), "full scan");
            db.awaitBackground();
            db.verify();
            Stats stats = db.stats();
            compactions += stats.compactions();
            System.out.printf("seed %d: %d reopens, %d compactions, deepest level %d, %d live keys; last session:%n%s%n",
                    seed, reopens, compactions, deepestLevel, model.size(), stats);
            assertTrue(compactions > 20, "compactions ran");
            assertTrue(deepestLevel >= 3, "data flowed down at least three levels");

            db.compactAll();
            db.verify();
            assertEquals(scan(model, null, null), scan(db, null, null), "after full compaction");
            assertEquals(0, db.stats().filesPerLevel().getFirst(), "level 0 empty after full compaction");
        } finally {
            db.close();
        }
    }
}
