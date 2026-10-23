package io.github.tendo505.strata;

import io.github.tendo505.strata.sstable.BlockCache;
import io.github.tendo505.strata.sstable.TableMeta;
import io.github.tendo505.strata.sstable.TableReader;
import io.github.tendo505.strata.sstable.TableWriter;
import io.github.tendo505.strata.util.Bytes;
import io.github.tendo505.strata.wal.LogReader;
import io.github.tendo505.strata.wal.LogWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * An LSM-tree key-value store.
 *
 * <p><b>Writes</b> append to a write-ahead log and go into an in-memory skip
 * list (the memtable). When the memtable is full it becomes immutable, a new
 * one takes over, and a flush thread writes the old one to disk as a
 * sorted, immutable SSTable in level 0. Flushing has its own thread so that a
 * long compaction never keeps a full memtable waiting (and writers with it).
 *
 * <p><b>Compaction</b> keeps reads fast: when level 0 has too many tables, or a
 * deeper level grows past its size limit, tables are merged into the next
 * level, newest values winning and deleted keys dropped once nothing older
 * could resurface. Each level is about ten times larger than the one above.
 *
 * <p><b>Reads</b> check the memtables, then level 0 newest first, then one table
 * per deeper level, skipping most tables via key ranges and Bloom filters.
 *
 * <p>One thread may write at a time (writes are serialised internally); any
 * number may read and scan concurrently.
 */
public final class Strata implements AutoCloseable {
    private static final Pattern NUMBERED = Pattern.compile("(\\d+)\\.(log|sst)");

    private final Path dir;
    private final Options options;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private final BlockCache cache;
    private final AtomicLong nextFileNumber;
    private final ConcurrentHashMap<Long, TableReader> readers = new ConcurrentHashMap<>();
    private final ExecutorService flusher = daemonThread("strata-flush");
    private final ExecutorService compactor = daemonThread("strata-compaction");

    // ---- guarded by writeLock ----
    private final Object writeLock = new Object();
    private LogWriter wal;
    private boolean flushScheduled;
    private boolean compactionScheduled;

    // ---- read without locks ----
    private volatile MemTable mem = new MemTable();
    private volatile MemTable imm;
    private volatile long immNextLogNumber;
    private volatile int level0Files;
    private volatile Throwable backgroundError;
    private volatile boolean closed;

    // ---- guarded by versionLock ----
    private final Object versionLock = new Object();
    private Manifest manifest;
    private Version current;
    private final Set<Version> liveVersions = Collections.newSetFromMap(new IdentityHashMap<>());
    private final Set<Long> obsolete = new TreeSet<>();
    private final byte[][] compactPointer = new byte[Options.LEVELS][];

    // ---- stats ----
    private final AtomicLong userBytes = new AtomicLong();
    private final AtomicLong tableBytes = new AtomicLong();
    private final AtomicLong flushes = new AtomicLong();
    private final AtomicLong compactions = new AtomicLong();
    private final AtomicLong trivialMoves = new AtomicLong();
    private final AtomicLong stalls = new AtomicLong();

    private Strata(Path dir, Options options, FileChannel lockChannel, FileLock lock, long nextFile) {
        this.dir = dir;
        this.options = options;
        this.lockChannel = lockChannel;
        this.lock = lock;
        this.cache = new BlockCache(options.blockCacheBytes());
        this.nextFileNumber = new AtomicLong(nextFile);
    }

    public static Strata open(Path dir) throws IOException {
        return open(dir, Options.defaults());
    }

    /**
     * Opens (or creates) a database. After a crash this replays the write-ahead
     * logs, so every write that returned before the crash is there again
     * (with {@link Options#syncWrites()}, even after a power failure).
     */
    public static Strata open(Path dir, Options options) throws IOException {
        Files.createDirectories(dir);
        FileChannel lockChannel = FileChannel.open(dir.resolve("LOCK"), StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        FileLock lock;
        try {
            lock = lockChannel.tryLock();
        } catch (IOException | OverlappingFileLockException e) {
            lock = null; // held by another process, or already open in this one
        }
        if (lock == null) {
            lockChannel.close();
            throw new IOException(dir + " is already open in another process");
        }
        try {
            Manifest.Recovered recovered = Manifest.recover(dir);
            long highest = 0;
            for (long n : numberedFiles(dir, "log")) highest = Math.max(highest, n);
            for (long n : numberedFiles(dir, "sst")) highest = Math.max(highest, n);
            long nextFile = Math.max(highest + 1, recovered == null ? 1 : recovered.nextFileNumber());
            Strata db = new Strata(dir, options, lockChannel, lock, nextFile);
            db.recover(recovered);
            return db;
        } catch (IOException | RuntimeException e) {
            lock.release();
            lockChannel.close();
            throw e;
        }
    }

    private void recover(Manifest.Recovered recovered) throws IOException {
        Version version = recovered == null ? Version.empty() : recovered.version();
        long logNumber = recovered == null ? 0 : recovered.logNumber();

        // Writes that never reached an SSTable are in the logs: replay them, oldest first.
        MemTable replayed = new MemTable();
        List<Long> oldLogs = numberedFiles(dir, "log");
        for (long n : oldLogs) {
            if (n < logNumber) continue;
            for (byte[] record : LogReader.read(logPath(n)).records()) {
                for (Entry e : WriteBatch.decode(record)) replayed.apply(e);
            }
        }
        VersionEdit edit = new VersionEdit();
        if (!replayed.isEmpty()) edit.add(0, writeTable(replayed.cursor(null), false));
        version = version.apply(edit);

        long walNumber = nextFileNumber.getAndIncrement();
        wal = new LogWriter(logPath(walNumber));
        long manifestNumber = nextFileNumber.getAndIncrement();
        manifest = Manifest.create(dir, manifestNumber, version, nextFileNumber.get(), walNumber);

        // Everything the old logs held is now in a table that the new manifest lists.
        for (long n : oldLogs) Files.deleteIfExists(logPath(n));
        // Tables no version mentions are leftovers of a flush or compaction that crashed half way.
        Set<Long> live = version.fileNumbers();
        for (long n : numberedFiles(dir, "sst")) if (!live.contains(n)) Files.deleteIfExists(tablePath(n));

        synchronized (versionLock) {
            install(version);
        }
        synchronized (writeLock) {
            scheduleCompaction();
        }
    }

    // ================= writes =================

    public void put(byte[] key, byte[] value) throws IOException {
        write(new WriteBatch().put(key, value));
    }

    public void delete(byte[] key) throws IOException {
        write(new WriteBatch().delete(key));
    }

    /** Applies a batch atomically: after a crash, all of it or none of it is there. */
    public void write(WriteBatch batch) throws IOException {
        if (batch.size() == 0) return;
        synchronized (writeLock) {
            ensureOpen();
            makeRoomForWrite();
            wal.append(batch.encode(), options.syncWrites());
            for (Entry e : batch.ops()) mem.apply(e);
            userBytes.addAndGet(batch.userBytes());
        }
    }

    /** Called holding writeLock: swaps in a fresh memtable when the current one is full, stalling if the background is behind. */
    private void makeRoomForWrite() throws IOException {
        boolean stalled = false;
        while (true) {
            throwIfBackgroundFailed();
            if (level0Files >= options.l0StopTrigger()) {
                // Too many overlapping tables make reads slow: wait for compaction to catch up.
                if (!stalled) stalls.incrementAndGet();
                stalled = true;
                scheduleCompaction();
                waitOnWriteLock();
            } else if (mem.approximateBytes() < options.memtableBytes()) {
                return;
            } else if (imm != null) {
                // The previous memtable is still being flushed.
                if (!stalled) stalls.incrementAndGet();
                stalled = true;
                waitOnWriteLock();
            } else {
                rotateMemtable();
                return;
            }
        }
    }

    private void rotateMemtable() throws IOException {
        long newLog = nextFileNumber.getAndIncrement();
        LogWriter next = new LogWriter(logPath(newLog));
        wal.close();
        wal = next;
        immNextLogNumber = newLog;
        imm = mem;
        mem = new MemTable();
        scheduleFlush();
    }

    private void waitOnWriteLock() throws IOException {
        try {
            writeLock.wait(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for compaction", e);
        }
    }

    /** Writes the memtable to a level-0 table now and waits for it. */
    public void flush() throws IOException {
        synchronized (writeLock) {
            ensureOpen();
            while (imm != null) {
                throwIfBackgroundFailed();
                waitOnWriteLock();
            }
            if (!mem.isEmpty()) rotateMemtable();
            while (imm != null) {
                throwIfBackgroundFailed();
                waitOnWriteLock();
            }
        }
    }

    /**
     * Flushes, then merges every level down into the deepest level that holds
     * data (as LevelDB's CompactRange does), leaving one sorted run with no
     * tombstones.
     */
    public void compactAll() throws IOException {
        flush();
        onCompactionThread(() -> {
            int deepest = 1;
            synchronized (versionLock) {
                for (int level = 1; level < Options.LEVELS; level++) if (!current.levels.get(level).isEmpty()) deepest = level;
            }
            for (int level = 0; level < deepest; level++) {
                Version v = acquire();
                try {
                    List<TableMeta> inputs = v.levels.get(level);
                    if (inputs.isEmpty()) continue;
                    List<TableMeta> next = v.overlapping(level + 1, smallestOf(inputs), largestOf(inputs));
                    compact(level, inputs, next, v, false);
                } finally {
                    release(v);
                }
            }
            return null;
        });
    }

    // ================= reads =================

    /** The value for {@code key}, or null if there is none. */
    public byte[] get(byte[] key) throws IOException {
        ensureOpen();
        // Order matters: memtable, then immutable memtable, then tables. A flush installs its table
        // before clearing imm, so data is never missing from all three at once.
        byte[] v = mem.get(key);
        if (v != null) return v == MemTable.TOMBSTONE ? null : v.clone();
        MemTable frozen = imm;
        if (frozen != null && (v = frozen.get(key)) != null) return v == MemTable.TOMBSTONE ? null : v.clone();

        Version version = acquire();
        try {
            for (TableMeta f : version.levels.get(0)) {
                if (!f.contains(key)) continue;
                Entry e = reader(f).get(key);
                if (e != null) return e.value() == null ? null : e.value().clone();
            }
            for (int level = 1; level < Options.LEVELS; level++) {
                TableMeta f = version.find(level, key);
                if (f == null) continue;
                Entry e = reader(f).get(key);
                if (e != null) return e.value() == null ? null : e.value().clone();
            }
            return null;
        } catch (UncheckedIOException e) {
            throw e.getCause();
        } finally {
            release(version);
        }
    }

    /**
     * Entries with {@code from <= key < to} in key order (null bounds are open).
     * Close the scan when done: it pins the tables it reads.
     */
    public Scan scan(byte[] from, byte[] to) throws IOException {
        ensureOpen();
        List<Cursor> sources = new ArrayList<>();
        sources.add(mem.cursor(from));
        MemTable frozen = imm;
        if (frozen != null) sources.add(frozen.cursor(from));
        Version version = acquire();
        for (TableMeta f : version.levels.get(0)) if (f.overlaps(from, null) && (to == null || Bytes.compare(f.smallest(), to) < 0)) sources.add(reader(f).cursor(from));
        for (int level = 1; level < Options.LEVELS; level++) {
            for (TableMeta f : version.levels.get(level)) {
                if (f.overlaps(from, null) && (to == null || Bytes.compare(f.smallest(), to) < 0)) sources.add(reader(f).cursor(from));
            }
        }
        return new Scan(new MergingCursor(sources), to, version);
    }

    /** An iterator over a key range. Deleted keys are skipped. */
    public final class Scan implements Iterator<Entry>, AutoCloseable {
        private final Cursor cursor;
        private final byte[] to;
        private Version version;

        private Scan(Cursor cursor, byte[] to, Version version) {
            this.cursor = cursor;
            this.to = to;
            this.version = version;
            skipTombstones();
        }

        private void skipTombstones() {
            while (cursor.valid() && cursor.value() == null && (to == null || Bytes.compare(cursor.key(), to) < 0)) cursor.next();
        }

        @Override
        public boolean hasNext() {
            return version != null && cursor.valid() && (to == null || Bytes.compare(cursor.key(), to) < 0);
        }

        @Override
        public Entry next() {
            if (!hasNext()) throw new NoSuchElementException();
            Entry e = new Entry(cursor.key().clone(), cursor.value().clone());
            cursor.next();
            skipTombstones();
            return e;
        }

        @Override
        public void close() {
            if (version != null) release(version);
            version = null;
        }
    }

    // ================= background: flush and compaction =================

    /** Called holding writeLock. */
    private void scheduleFlush() {
        if (flushScheduled || closed || backgroundError != null) return;
        flushScheduled = true;
        flusher.execute(this::flushWork);
    }

    /** Called holding writeLock. */
    private void scheduleCompaction() {
        if (compactionScheduled || closed || backgroundError != null) return;
        compactionScheduled = true;
        compactor.execute(this::compactionWork);
    }

    private void flushWork() {
        try {
            flushImmutable();
        } catch (Throwable t) {
            backgroundError = t;
        } finally {
            synchronized (writeLock) {
                flushScheduled = false;
                if (imm != null) scheduleFlush();
                scheduleCompaction(); // a new level-0 table may need merging
                writeLock.notifyAll();
            }
        }
    }

    private void compactionWork() {
        try {
            while (!closed) {
                if (!compactOnce()) break;
                synchronized (writeLock) {
                    writeLock.notifyAll(); // level 0 shrank: stalled writers may proceed
                }
            }
        } catch (Throwable t) {
            backgroundError = t;
        } finally {
            synchronized (writeLock) {
                compactionScheduled = false;
                if (needsCompaction()) scheduleCompaction();
                writeLock.notifyAll();
            }
        }
    }

    private void flushImmutable() throws IOException {
        MemTable frozen = imm;
        if (frozen == null) return;
        VersionEdit edit = new VersionEdit();
        if (!frozen.isEmpty()) edit.add(0, writeTable(frozen.cursor(null), true));
        edit.logNumber = immNextLogNumber; // older logs are now fully in tables
        logAndApply(edit);
        flushes.incrementAndGet();
        synchronized (writeLock) {
            imm = null;
            writeLock.notifyAll();
        }
        for (long n : numberedFiles(dir, "log")) if (n < edit.logNumber) Files.deleteIfExists(logPath(n));
    }

    private boolean needsCompaction() {
        synchronized (versionLock) {
            return pick(current) != null;
        }
    }

    private record Pick(int level, List<TableMeta> inputs, List<TableMeta> next) {}

    /** Level 0 by file count; deeper levels by size, round-robin through the key space. */
    private Pick pick(Version v) {
        List<TableMeta> l0 = v.levels.get(0);
        if (l0.size() >= options.l0CompactionTrigger()) {
            return new Pick(0, l0, v.overlapping(1, smallestOf(l0), largestOf(l0)));
        }
        for (int level = 1; level < Options.LEVELS - 1; level++) {
            if (v.levelBytes(level) <= options.maxBytesForLevel(level)) continue;
            List<TableMeta> files = v.levels.get(level);
            TableMeta chosen = files.getFirst();
            byte[] after = compactPointer[level];
            if (after != null) {
                for (TableMeta f : files) {
                    if (Bytes.compare(f.smallest(), after) > 0) {
                        chosen = f;
                        break;
                    }
                }
            }
            return new Pick(level, List.of(chosen), v.overlapping(level + 1, chosen.smallest(), chosen.largest()));
        }
        return null;
    }

    private boolean compactOnce() throws IOException {
        Version v = acquire();
        try {
            Pick p;
            synchronized (versionLock) {
                p = pick(v);
            }
            if (p == null) return false;
            compact(p.level(), p.inputs(), p.next(), v, true);
            return true;
        } finally {
            release(v);
        }
    }

    /**
     * Merges {@code inputs} (from {@code level}) with the overlapping tables of the
     * next level into new tables for the next level.
     */
    private void compact(int level, List<TableMeta> inputs, List<TableMeta> next, Version v, boolean allowMove) throws IOException {
        VersionEdit edit = new VersionEdit();
        compactPointer[level] = largestOf(inputs);
        if (allowMove && level > 0 && inputs.size() == 1 && next.isEmpty()) {
            // Nothing to merge with: move the file down a level without rewriting it.
            TableMeta f = inputs.getFirst();
            edit.remove(level, f.number()).add(level + 1, f);
            logAndApply(edit);
            trivialMoves.incrementAndGet();
            compactions.incrementAndGet();
            return;
        }
        List<Cursor> sources = new ArrayList<>();
        for (TableMeta f : inputs) sources.add(reader(f).cursor(null)); // level 0 is newest first already
        for (TableMeta f : next) sources.add(reader(f).cursor(null));
        MergingCursor merged = new MergingCursor(sources);
        int output = level + 1;

        TableWriter writer = null;
        long number = 0;
        try {
            while (merged.valid()) {
                byte[] key = merged.key();
                byte[] value = merged.value();
                // A tombstone can go once no deeper level could still hold an older value for the key.
                boolean drop = value == null && isBottommost(v, output, key);
                if (!drop) {
                    if (writer == null) {
                        number = nextFileNumber.getAndIncrement();
                        writer = new TableWriter(tablePath(number), options.blockBytes(), options.bloomBitsPerKey());
                    }
                    writer.add(key, value);
                    if (writer.estimatedSize() >= options.targetFileBytes()) {
                        TableMeta meta = writer.finish(number);
                        tableBytes.addAndGet(meta.size());
                        edit.add(output, meta);
                        writer = null;
                    }
                }
                merged.next();
            }
            if (writer != null) {
                TableMeta meta = writer.finish(number);
                tableBytes.addAndGet(meta.size());
                edit.add(output, meta);
                writer = null;
            }
        } catch (IOException | RuntimeException e) {
            if (writer != null) {
                writer.close();
                Files.deleteIfExists(tablePath(number));
            }
            throw e;
        }
        for (TableMeta f : inputs) edit.remove(level, f.number());
        for (TableMeta f : next) edit.remove(output, f.number());
        logAndApply(edit);
        compactions.incrementAndGet();
    }

    private static boolean isBottommost(Version v, int outputLevel, byte[] key) {
        for (int level = outputLevel + 1; level < Options.LEVELS; level++) {
            for (TableMeta f : v.levels.get(level)) if (f.contains(key)) return false;
        }
        return true;
    }

    private TableMeta writeTable(Cursor c, boolean countBytes) throws IOException {
        long number = nextFileNumber.getAndIncrement();
        try (TableWriter w = new TableWriter(tablePath(number), options.blockBytes(), options.bloomBitsPerKey())) {
            for (; c.valid(); c.next()) w.add(c.key(), c.value());
            TableMeta meta = w.finish(number);
            if (countBytes) tableBytes.addAndGet(meta.size());
            return meta;
        }
    }

    // ================= versions and files =================

    private void logAndApply(VersionEdit edit) throws IOException {
        synchronized (versionLock) {
            edit.nextFileNumber = nextFileNumber.get();
            manifest.append(edit); // durable before anything relies on it
            install(current.apply(edit));
            for (VersionEdit.Removed r : edit.removed) obsolete.add(r.number());
            for (VersionEdit.Added a : edit.added) obsolete.remove(a.meta().number()); // a trivial move keeps its file
            deleteObsoleteFiles();
        }
    }

    /** Called holding versionLock. The database itself holds one reference to the current version. */
    private void install(Version v) {
        Version old = current;
        v.refs++;
        liveVersions.add(v);
        current = v;
        level0Files = v.levels.get(0).size();
        if (old != null) unref(old);
    }

    private Version acquire() {
        synchronized (versionLock) {
            current.refs++;
            return current;
        }
    }

    private void release(Version v) {
        synchronized (versionLock) {
            unref(v);
        }
    }

    private void unref(Version v) {
        if (--v.refs == 0) {
            liveVersions.remove(v);
            deleteObsoleteFiles();
        }
    }

    /** Deletes replaced tables once no reader's version still lists them. Called holding versionLock. */
    private void deleteObsoleteFiles() {
        if (obsolete.isEmpty()) return;
        Set<Long> referenced = new TreeSet<>();
        for (Version v : liveVersions) referenced.addAll(v.fileNumbers());
        for (Iterator<Long> it = obsolete.iterator(); it.hasNext(); ) {
            long n = it.next();
            if (referenced.contains(n)) continue;
            TableReader r = readers.remove(n);
            try {
                if (r != null) r.close();
                Files.deleteIfExists(tablePath(n));
            } catch (IOException e) {
                // A file we can't delete yet (e.g. still open elsewhere on Windows) is retried at the next open.
            }
            it.remove();
        }
    }

    private TableReader reader(TableMeta f) {
        return readers.computeIfAbsent(f.number(), n -> {
            try {
                return TableReader.open(tablePath(n), n, cache);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

    // ================= odds and ends =================

    public Stats stats() {
        synchronized (versionLock) {
            List<Integer> files = new ArrayList<>();
            List<Long> bytes = new ArrayList<>();
            for (int level = 0; level < Options.LEVELS; level++) {
                files.add(current.levels.get(level).size());
                bytes.add(current.levelBytes(level));
            }
            MemTable frozen = imm;
            return new Stats(List.copyOf(files), List.copyOf(bytes), mem.approximateBytes() + (frozen == null ? 0 : frozen.approximateBytes()),
                    userBytes.get(), tableBytes.get(), flushes.get(), compactions.get(), trivialMoves.get(), stalls.get(), cache.hitRate());
        }
    }

    /** Checks structural invariants of the current version (sorted, non-overlapping levels). */
    public void verify() {
        synchronized (versionLock) {
            current.verify();
        }
    }

    /** Waits until no flush or compaction is pending. */
    public void awaitBackground() throws IOException {
        synchronized (writeLock) {
            while (flushScheduled || compactionScheduled || imm != null) {
                throwIfBackgroundFailed();
                waitOnWriteLock();
            }
        }
        throwIfBackgroundFailed();
    }

    /** Runs a task on the compaction thread, so it never races an automatic compaction, and waits for it. */
    private <T> T onCompactionThread(Callable<T> task) throws IOException {
        try {
            return compactor.submit(task).get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException io) throw io;
            if (e.getCause() instanceof UncheckedIOException u) throw u.getCause();
            throw new IOException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("database is closed");
    }

    private void throwIfBackgroundFailed() throws IOException {
        Throwable t = backgroundError;
        if (t != null) throw new IOException("background flush or compaction failed; the database is read-only until reopened", t);
    }

    /**
     * Closes the database. Unflushed writes stay in the write-ahead log and are
     * replayed on the next open, exactly as after a crash.
     */
    @Override
    public void close() throws IOException {
        synchronized (writeLock) {
            if (closed) return;
            closed = true;
        }
        flusher.shutdown();
        compactor.shutdown();
        try {
            if (!flusher.awaitTermination(1, TimeUnit.MINUTES) || !compactor.awaitTermination(1, TimeUnit.MINUTES)) {
                throw new IOException("background work did not stop");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        synchronized (writeLock) {
            wal.sync();
            wal.close();
        }
        synchronized (versionLock) {
            manifest.close();
        }
        for (TableReader r : readers.values()) r.close();
        readers.clear();
        lock.release();
        lockChannel.close();
    }

    private static ExecutorService daemonThread(String name) {
        return Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        });
    }

    private Path logPath(long n) {
        return dir.resolve(String.format("%06d.log", n));
    }

    private Path tablePath(long n) {
        return dir.resolve(TableMeta.fileName(n));
    }

    private static List<Long> numberedFiles(Path dir, String extension) throws IOException {
        List<Long> out = new ArrayList<>();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.toList()) {
                Matcher m = NUMBERED.matcher(p.getFileName().toString());
                if (m.matches() && m.group(2).equals(extension)) out.add(Long.parseLong(m.group(1)));
            }
        }
        Collections.sort(out);
        return out;
    }

    private static byte[] smallestOf(List<TableMeta> files) {
        byte[] min = files.getFirst().smallest();
        for (TableMeta f : files) if (Bytes.compare(f.smallest(), min) < 0) min = f.smallest();
        return min;
    }

    private static byte[] largestOf(List<TableMeta> files) {
        byte[] max = files.getFirst().largest();
        for (TableMeta f : files) if (Bytes.compare(f.largest(), max) > 0) max = f.largest();
        return max;
    }
}
