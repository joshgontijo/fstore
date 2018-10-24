package io.joshworks.fstore.log.segment.footer;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;

import java.nio.ByteBuffer;

public class LogFooter {

    public static final int BYTES = 1024;

    public final long sealedDate;
    public final long logEnd;
    public final long logSize;
    public final long entries;
    public final int level;

    private LogFooter(long sealedDate, long logEnd, long logSize, long entries, int level) {
        this.sealedDate = sealedDate;
        this.logEnd = logEnd;
        this.logSize = logSize;
        this.entries = entries;
        this.level = level;
    }

    public static LogFooter read(Storage storage, long position) {
        ByteBuffer bb = ByteBuffer.allocate(LogFooter.BYTES);
        storage.read(position, bb);
        bb.flip();
        if (bb.remaining() == 0) {
            return null;
        }
        int length = bb.getInt();
        if (length == 0) {
            return null;
        }
        int checksum = bb.getInt();
        bb.limit(bb.position() + length); //length + checksum
        if (Checksum.crc32(bb) != checksum) {
            throw new IllegalStateException("Log head checksum verification failed");
        }

        long sealedDate = bb.getLong();
        long logEnd = bb.getLong();
        long actualLogSize = bb.getLong();
        long entries = bb.getLong();
        int level = bb.getInt();

        return new LogFooter(sealedDate, logEnd, actualLogSize, entries, level);
    }

    public static LogFooter write(Storage storage, long logEnd, long sealedDate, long actualLogSize, long entries, int level) {
        try {

            ByteBuffer headerData = ByteBuffer.allocate(BYTES);
            headerData.putLong(sealedDate);
            headerData.putLong(logEnd);
            headerData.putLong(actualLogSize);
            headerData.putLong(entries);
            headerData.putInt(level);
            headerData.flip();

            int entrySize = headerData.remaining();

            ByteBuffer withChecksumAndLength = ByteBuffer.allocate(BYTES);
            withChecksumAndLength.putInt(entrySize);
            withChecksumAndLength.putInt(Checksum.crc32(headerData));
            withChecksumAndLength.put(headerData);
            withChecksumAndLength.position(0);//do not flip, the header will always have the fixed size

            long pos = storage.position();
            if(pos < logEnd) {
                throw new IllegalStateException("Current log position is less than logEnd");
            }
            storage.position(logEnd);
            if (storage.write(withChecksumAndLength) != LogFooter.BYTES) {
                throw new IllegalStateException("Unexpected written header length");
            }
            return new LogFooter(sealedDate, logEnd, actualLogSize, entries, level);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }


}
