package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;

import java.nio.ByteBuffer;

public class LogHeader {

    public static final int BYTES = 1024;

    public final long magic;
    public final long created;
    public final long logSize;
    public final long footerPos;
    public final Type type;

    private LogHeader(long magic, long created,  long logSize, long footerPos, Type type) {
        this.magic = magic;
        this.created = created;
        this.logSize = logSize;
        this.footerPos = footerPos;
        this.type = type;
    }

    public static LogHeader read(Storage storage) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
        storage.read(0, bb);
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

        long magic = bb.getLong();
        long created = bb.getLong();
        long logSize = bb.getLong();
        long footerPos = bb.getLong();
        int typeCode = bb.getInt();

        return new LogHeader(magic, created, logSize, footerPos, Type.of(typeCode));
    }

    public static LogHeader write(Storage storage, long magic, long created, long logSize, long footerPos, Type type) {
        try {
            ByteBuffer headerData = ByteBuffer.allocate(BYTES);
            headerData.putLong(magic);
            headerData.putLong(created);
            headerData.putLong(logSize);
            headerData.putLong(footerPos);
            headerData.putInt(type.val);
            headerData.flip();

            int entrySize = headerData.remaining();

            ByteBuffer withChecksumAndLength = ByteBuffer.allocate(BYTES);
            withChecksumAndLength.putInt(entrySize);
            withChecksumAndLength.putInt(Checksum.crc32(headerData));
            withChecksumAndLength.put(headerData);
            withChecksumAndLength.position(0);//do not flip, the header will always have the fixed size

            if (storage.position() != 0) {
                throw new IllegalStateException("Storage position is not at the beginning of the log");
            }
            if (storage.write(withChecksumAndLength) != LogHeader.BYTES) {
                throw new IllegalStateException("Unexpected written header length");
            }
            return new LogHeader(magic, created, logSize, footerPos, type);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }


}
