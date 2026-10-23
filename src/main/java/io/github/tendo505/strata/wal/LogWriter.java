package io.github.tendo505.strata.wal;

import io.github.tendo505.strata.util.Bytes;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * An append-only file of checksummed records, used for the write-ahead log
 * and the manifest. Each record is framed as
 *
 * <pre>  crc32c(length ‖ payload) : 4 bytes | length : 4 bytes | payload</pre>
 *
 * so a reader can tell a complete record from one torn by a crash.
 */
public final class LogWriter implements Closeable {
    private final FileChannel channel;
    private final Path path;

    public LogWriter(Path path) throws IOException {
        this.path = path;
        this.channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND);
    }

    public Path path() {
        return path;
    }

    /**
     * Appends one record. Once this returns the bytes are in the operating
     * system's hands and survive the process crashing; with {@code sync} they
     * are also on the device and survive the machine losing power.
     */
    public void append(byte[] payload, boolean sync) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(8 + payload.length);
        buf.putInt(0).putInt(payload.length).put(payload);
        buf.putInt(0, Bytes.crc(buf.array(), 4, 4 + payload.length));
        buf.flip();
        while (buf.hasRemaining()) channel.write(buf);
        if (sync) channel.force(false);
    }

    public void sync() throws IOException {
        channel.force(false);
    }

    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }
}
