package io.github.tendo505.strata.wal;

import io.github.tendo505.strata.util.Bytes;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/** Reads a {@link LogWriter} file back, stopping cleanly at a torn or corrupt tail. */
public final class LogReader {

    /**
     * The records that were read intact. {@code clean} is false if the file ended
     * with a partial or corrupt record (normal after a crash mid-write); that
     * record and anything after it are ignored.
     */
    public record Result(List<byte[]> records, long validBytes, boolean clean) {}

    private LogReader() {}

    public static Result read(Path path) throws IOException {
        byte[] data = Files.readAllBytes(path);
        ByteBuffer in = ByteBuffer.wrap(data);
        List<byte[]> records = new ArrayList<>();
        while (in.remaining() >= 8) {
            int start = in.position();
            int crc = in.getInt();
            int length = in.getInt();
            if (length < 0 || length > in.remaining()) return new Result(records, start, false);
            if (Bytes.crc(data, start + 4, 4 + length) != crc) return new Result(records, start, false);
            byte[] payload = new byte[length];
            in.get(payload);
            records.add(payload);
        }
        return new Result(records, in.position(), !in.hasRemaining());
    }
}
