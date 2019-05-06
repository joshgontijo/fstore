package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.ByteBufferChecksum;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LogHeader {

    private static final Serializer<LogHeader> headerSerializer = new HeaderSerializer();
    public static final int BYTES = 1024;

    //newHeader
    public final String magic;
    public final long created;
    public final Type type;
    public final long fileSize;
    public final boolean encrypted;

    //completed info
    public final int level; //segments created are implicit level zero
    public final long entries;
    public final long writePosition; //actual written bytes, including header
    public final long rolled;
    public final long uncompressedSize;

    LogHeader(String magic, long entries, long created, int level, Type type, long rolled, long fileSize, boolean encrypted, long writePosition, long uncompressedSize) {
        this.magic = magic;
        this.entries = entries;
        this.created = created;
        this.level = level;
        this.type = type;
        this.rolled = rolled;
        this.fileSize = fileSize;
        this.encrypted = encrypted;
        this.writePosition = writePosition;
        this.uncompressedSize = uncompressedSize;
    }


    public static void validateMagic(String actualMagic, String expectedMagic) {
        byte[] actual = actualMagic.getBytes(StandardCharsets.UTF_8);
        byte[] expected = expectedMagic.getBytes(StandardCharsets.UTF_8);
        if (!Arrays.equals(expected, actual)) {
            throw new InvalidMagic(expectedMagic, actualMagic);
        }
    }

    public static LogHeader read(Storage storage) {
        if (storage.writePosition() < LogHeader.BYTES) {
            storage.writePosition(LogHeader.BYTES);
        }
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
        if (ByteBufferChecksum.crc32(bb) != checksum) {
            throw new IllegalStateException("Log head checksum verification failed");
        }

        return headerSerializer.fromBytes(bb);
    }

    public static LogHeader writeNew(Storage storage, String magic, Type type, long fileSize, boolean encrypted) {
        LogHeader newHeader = new LogHeader(magic, 0, System.currentTimeMillis(), 0, type, 0, fileSize, encrypted, BYTES, 0);
        write(storage, newHeader);
        return newHeader;
    }

    public static LogHeader writeCompleted(Storage storage, LogHeader initialHeader, long entries, int level, long logicalSize, long uncompressedSize) {
        LogHeader newHeader = new LogHeader(
                initialHeader.magic,
                entries,
                initialHeader.created,
                level,
                Type.READ_ONLY,
                System.currentTimeMillis(),
                initialHeader.fileSize,
                initialHeader.encrypted,
                logicalSize,
                uncompressedSize);
        write(storage, newHeader);
        return newHeader;
    }

    public static LogHeader writeDeleted(Storage storage, LogHeader initialHeader) {
        LogHeader newHeader = new LogHeader(
                initialHeader.magic,
                -1,
                initialHeader.created,
                -1,
                Type.READ_ONLY,
                System.currentTimeMillis(),
                initialHeader.fileSize,
                initialHeader.encrypted, -1,
                initialHeader.uncompressedSize);

        write(storage, newHeader);
        return newHeader;
    }


    private static LogHeader write(Storage storage, LogHeader header) {
        try {
            ByteBuffer withChecksumAndLength = ByteBuffer.allocate(BYTES);
            ByteBuffer headerData = headerSerializer.toBytes(header);

            int entrySize = headerData.remaining();
            withChecksumAndLength.putInt(entrySize);
            withChecksumAndLength.putInt(ByteBufferChecksum.crc32(headerData));
            withChecksumAndLength.put(headerData);
            withChecksumAndLength.position(0);//do not flip, the header will always have the fixed size

            long prevPos = storage.writePosition();
            storage.writePosition(0);
            if (storage.write(withChecksumAndLength) != LogHeader.BYTES) {
                throw new IllegalStateException("Unexpected written header length");
            }
            storage.writePosition(prevPos);
            return header;
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }

    @Override
    public String toString() {
        return "LogHeader{" + "magic='" + magic + '\'' +
                ", created=" + created +
                ", type=" + type +
                ", fileSize=" + fileSize +
                ", level=" + level +
                ", entries=" + entries +
                ", logicalSize=" + writePosition +
                ", rolled=" + rolled +
                '}';
    }
}
