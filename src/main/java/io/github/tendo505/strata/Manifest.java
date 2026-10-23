package io.github.tendo505.strata;

import io.github.tendo505.strata.sstable.TableMeta;
import io.github.tendo505.strata.wal.LogReader;
import io.github.tendo505.strata.wal.LogWriter;
import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.stream.Stream;

/**
 * The manifest is a log of {@link VersionEdit}s; replaying it rebuilds the
 * current set of tables. The file {@code CURRENT} names the live manifest and
 * is replaced atomically (write a temporary file, fsync, rename), so a crash
 * at any moment leaves either the old manifest or the new one in force.
 */
final class Manifest implements Closeable {
    static final String CURRENT = "CURRENT";

    record Recovered(Version version, long nextFileNumber, long logNumber, String manifestName) {}

    private final LogWriter log;

    private Manifest(LogWriter log) {
        this.log = log;
    }

    /** Rebuilds the last saved version, or returns null for a new database. */
    static Recovered recover(Path dir) throws IOException {
        Path current = dir.resolve(CURRENT);
        if (!Files.exists(current)) return null;
        String name = Files.readString(current, StandardCharsets.UTF_8).trim();
        Path path = dir.resolve(name);
        if (!Files.exists(path)) throw new IOException("CURRENT names " + name + ", which is missing");
        // A torn final edit means its flush or compaction never completed: ignoring it is exactly right.
        List<byte[]> records = LogReader.read(path).records();
        Version version = Version.empty();
        long nextFile = 1;
        long logNumber = 0;
        for (byte[] record : records) {
            VersionEdit edit = VersionEdit.decode(record);
            version = version.apply(edit);
            nextFile = Math.max(nextFile, edit.nextFileNumber);
            if (edit.logNumber != 0) logNumber = edit.logNumber;
        }
        return new Recovered(version, nextFile, logNumber, name);
    }

    /** Starts a new manifest holding a snapshot of {@code version}, points CURRENT at it, and deletes older manifests. */
    static Manifest create(Path dir, long number, Version version, long nextFileNumber, long logNumber) throws IOException {
        String name = String.format("MANIFEST-%06d", number);
        LogWriter log = new LogWriter(dir.resolve(name));
        VersionEdit snapshot = new VersionEdit();
        for (int level = 0; level < Options.LEVELS; level++) {
            for (TableMeta f : version.levels.get(level)) snapshot.add(level, f);
        }
        snapshot.nextFileNumber = nextFileNumber;
        snapshot.logNumber = logNumber;
        log.append(snapshot.encode(), true);

        Path tmp = dir.resolve(CURRENT + ".tmp");
        Files.writeString(tmp, name + "\n", StandardCharsets.UTF_8);
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
            ch.force(true);
        }
        Files.move(tmp, dir.resolve(CURRENT), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        syncDirectory(dir);
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.toList()) {
                String f = p.getFileName().toString();
                if (f.startsWith("MANIFEST-") && !f.equals(name)) Files.deleteIfExists(p);
            }
        }
        return new Manifest(log);
    }

    void append(VersionEdit edit) throws IOException {
        log.append(edit.encode(), true);
    }

    long size() throws IOException {
        return log.size();
    }

    /** Makes a rename durable on POSIX systems; Windows can't open directories, and doesn't need this. */
    static void syncDirectory(Path dir) {
        try (FileChannel ch = FileChannel.open(dir, StandardOpenOption.READ)) {
            ch.force(true);
        } catch (IOException | UnsupportedOperationException ignored) {
            // not supported on this platform
        }
    }

    @Override
    public void close() throws IOException {
        log.close();
    }
}
