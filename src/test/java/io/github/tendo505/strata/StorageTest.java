package io.github.tendo505.strata;

import static io.github.tendo505.strata.util.Bytes.string;
import static io.github.tendo505.strata.util.Bytes.utf8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.tendo505.strata.sstable.BlockCache;
import io.github.tendo505.strata.sstable.TableMeta;
import io.github.tendo505.strata.sstable.TableReader;
import io.github.tendo505.strata.sstable.TableWriter;
import io.github.tendo505.strata.wal.LogReader;
import io.github.tendo505.strata.wal.LogWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StorageTest {
    @TempDir
    Path dir;

    @Test
    void sstablesRoundTripAndSeek() throws IOException {
        Path path = dir.resolve("t.sst");
        TableMeta meta;
        try (TableWriter w = new TableWriter(path, 256, 10)) {
            for (int i = 0; i < 5000; i++) w.add(utf8(String.format("k%06d", i * 2)), i % 10 == 0 ? null : utf8("value " + i));
            meta = w.finish(1);
        }
        assertEquals(5000, meta.entries());
        assertEquals("k000000", string(meta.smallest()));
        assertEquals("k009998", string(meta.largest()));
        try (TableReader r = TableReader.open(path, 1, new BlockCache(1 << 20))) {
            assertEquals("value 7", string(r.get(utf8("k000014")).value()));
            assertNull(r.get(utf8("k000020")).value(), "tombstones are stored and returned");
            assertNull(r.get(utf8("k000015")), "odd keys were never written");
            // Seek to a key between two entries, then iterate across many blocks.
            Cursor c = r.cursor(utf8("k004001"));
            assertEquals("k004002", string(c.key()));
            int n = 0;
            for (; c.valid(); c.next()) n++;
            assertEquals(5000 - 2001, n);
            assertFalse(r.cursor(utf8("z")).valid());
        }
    }

    @Test
    void bloomFiltersSkipMostAbsentKeys() throws IOException {
        Path path = dir.resolve("b.sst");
        try (TableWriter w = new TableWriter(path, 4096, 10)) {
            for (int i = 0; i < 20_000; i++) w.add(utf8(String.format("present%06d", i)), utf8("v"));
            w.finish(1);
        }
        try (TableReader r = TableReader.open(path, 1, new BlockCache(0))) {
            for (int i = 0; i < 20_000; i++) assertTrue(r.mightContain(utf8(String.format("present%06d", i))), "no false negatives");
            int passed = 0;
            for (int i = 0; i < 20_000; i++) if (r.mightContain(utf8(String.format("absent%06d", i)))) passed++;
            double rate = passed / 20_000.0;
            System.out.printf("Bloom filter false positive rate at 10 bits/key: %.2f%%%n", rate * 100);
            assertTrue(rate < 0.02, "false positive rate " + rate);
        }
    }

    @Test
    void corruptTablesFailLoudlyInsteadOfReturningGarbage() throws IOException {
        Path path = dir.resolve("c.sst");
        try (TableWriter w = new TableWriter(path, 256, 10)) {
            for (int i = 0; i < 1000; i++) w.add(utf8(String.format("k%04d", i)), utf8("value " + i));
            w.finish(1);
        }
        try (FileChannel ch = FileChannel.open(path, StandardOpenOption.WRITE)) {
            ch.write(java.nio.ByteBuffer.wrap(new byte[] {0x42}), 100); // flip a byte inside the first data block
        }
        try (TableReader r = TableReader.open(path, 1, new BlockCache(0))) {
            UncheckedIOException e = assertThrows(UncheckedIOException.class, () -> r.get(utf8("k0000")));
            assertTrue(e.getCause().getMessage().contains("checksum mismatch"), e.getCause().getMessage());
            assertEquals("value 999", string(r.get(utf8("k0999")).value()), "other blocks are still readable");
        }
    }

    /** A crash can cut the last WAL record at any byte. Recovery must keep exactly the complete records. */
    @Test
    void walRecoveryStopsAtATornRecordWhereverItIsCut() throws IOException {
        Path log = dir.resolve("w.log");
        List<byte[]> written = new ArrayList<>();
        try (LogWriter w = new LogWriter(log)) {
            for (int i = 0; i < 5; i++) {
                byte[] payload = utf8("record " + i + " " + "x".repeat(i * 7));
                written.add(payload);
                w.append(payload, false);
            }
        }
        byte[] full = Files.readAllBytes(log);
        long[] ends = new long[5];
        long at = 0;
        for (int i = 0; i < 5; i++) ends[i] = at += 8 + written.get(i).length;
        for (int cut = 0; cut <= full.length; cut++) {
            Path torn = dir.resolve("torn.log");
            Files.write(torn, java.util.Arrays.copyOf(full, cut));
            LogReader.Result r = LogReader.read(torn);
            int complete = 0;
            while (complete < 5 && ends[complete] <= cut) complete++;
            assertEquals(complete, r.records().size(), "cut at " + cut);
            for (int i = 0; i < complete; i++) assertArrayEquals(written.get(i), r.records().get(i));
            assertEquals(cut == full.length || (complete > 0 && ends[complete - 1] == cut) || cut == 0, r.clean(), "cut at " + cut);
        }
        // A flipped bit in the middle is caught by the checksum, and everything after it is ignored.
        full[(int) ends[1] + 10] ^= 1;
        Files.write(log, full);
        assertEquals(2, LogReader.read(log).records().size());
    }

    @Test
    void unflushedWritesSurviveReopenAndDeletesStayDeleted() throws IOException {
        try (Strata db = Strata.open(dir, Options.tiny())) {
            for (int i = 0; i < 3000; i++) db.put(utf8("k" + i), utf8("v" + i));
            for (int i = 0; i < 3000; i += 3) db.delete(utf8("k" + i));
        } // closed without flushing: the newest writes live only in the WAL
        try (Strata db = Strata.open(dir, Options.tiny())) {
            for (int i = 0; i < 3000; i++) {
                assertEquals(i % 3 == 0 ? null : "v" + i, string(db.get(utf8("k" + i))), "k" + i);
            }
        }
    }

    @Test
    void onlyOneProcessCanOpenADatabase() throws IOException {
        try (Strata db = Strata.open(dir, Options.tiny())) {
            db.put(utf8("a"), utf8("1"));
            IOException e = assertThrows(IOException.class, () -> Strata.open(dir, Options.tiny()));
            assertTrue(e.getMessage().contains("already open"), e.getMessage());
        }
    }

    @Test
    void scansSeeAConsistentSetOfTablesWhileCompactionReplacesThem() throws Exception {
        try (Strata db = Strata.open(dir, Options.tiny())) {
            for (int i = 0; i < 2000; i++) db.put(utf8(String.format("k%05d", i)), utf8("v" + i));
            db.flush();
            try (Strata.Scan scan = db.scan(null, null)) {
                // Rewrite everything underneath the open scan.
                for (int i = 0; i < 2000; i++) db.put(utf8(String.format("k%05d", i)), utf8("w" + i));
                db.compactAll();
                int n = 0;
                while (scan.hasNext()) {
                    scan.next();
                    n++;
                }
                assertEquals(2000, n, "the pinned tables were not deleted under the scan");
            }
            assertEquals("w1999", string(db.get(utf8("k01999"))));
        }
    }
}
