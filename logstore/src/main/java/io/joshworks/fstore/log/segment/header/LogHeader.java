package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.ByteBufferChecksum;
import io.joshworks.fstore.log.segment.CorruptedSegmentException;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class LogHeader {

    public static final int BYTES = 4096;
    public static final int SECTION_SIZE = 512; //data + checksum (effective available space SECTION_SIZE - 4bytes (checksum)

    public static final int SECTION_HEADER_BYTES = Integer.BYTES * 2; //length + checksum
    public static final int HEADER_START = 0;

    public static final int OPEN_SECTION_START = 0;
    public static final int COMPLETED_SECTION_START = OPEN_SECTION_START + SECTION_SIZE;
    public static final int MERGE_SECTION_START = COMPLETED_SECTION_START + SECTION_SIZE;
    public static final int DELETED_SECTION_START = MERGE_SECTION_START + SECTION_SIZE;

    public static final int UNKNOWN = -1;

    private OpenSection open;
    private CompletedSection completed;
    private DeletedSection deleted;

    private LogHeader() {

    }

    public Type type() {
        if (deleted != null) {
            return Type.DELETED;
        }
        if (completed != null) {
            return Type.READ_ONLY;
        }
        if (open != null) {
            return WriteMode.LOG_HEAD.equals(open.mode) ? Type.LOG_HEAD : Type.MERGE_OUT;
        }
        return Type.NONE;
    }


    public long created() {
        return open == null ? UNKNOWN : open.created;
    }

    public long fileSize() {
        return open == null ? UNKNOWN : open.fileSize;
    }

    public long dataSize() {
        if (completed != null) {
            return completed.actualDataSize;
        }
        if (open != null) {
            return open.dataSize;
        }
        return UNKNOWN;

    }

    public boolean encrypted() {
        return open != null && open.encrypted;
    }

    public long entries() {
        return completed == null ? 0 : completed.entries;
    }

    public int level() {
        if (open == null) {
            return UNKNOWN;
        }
        return completed == null ? 0 : completed.level;
    }

    public long rolled() {
        return completed == null ? UNKNOWN : completed.rolled;
    }

    public long uncompressedSize() {
        return completed == null ? UNKNOWN : completed.uncompressedSize;
    }

    public long writePosition() {
        if (completed == null) {
            return open == null ? 0 : BYTES;
        }
        return LogHeader.BYTES + completed.actualDataSize;
    }

    public long footerLength() {
        if (completed == null) {
            throw new IllegalStateException("No footer length available, segment is not readonly");
        }
        return completed.footerLength;
    }

    public static LogHeader read(Storage storage, String magic) {
        LogHeader header = new LogHeader();
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);

        storage.position(LogHeader.BYTES);
        storage.read(HEADER_START, bb);
        bb.flip();
        if (bb.remaining() == 0) {
            return header;
        }
        header.open = readOpenSection(bb, magic);
        header.completed = readCompleteSection(bb);
        header.deleted = readDeletedSection(bb);

        return header;
    }

    public void writeNew(Storage storage, WriteMode mode, long fileSize, long dataSize, boolean encrypted) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.SECTION_SIZE);
        long created = System.currentTimeMillis();

        bb.putLong(created);
        bb.putInt(mode.val);
        bb.putLong(fileSize);
        bb.putLong(dataSize);
        bb.putInt(encrypted ? 1 : 0);
        bb.flip();
        write(storage, OPEN_SECTION_START, bb);
        this.open = new OpenSection(created, mode, fileSize, dataSize, encrypted);
    }

    public void writeCompleted(Storage storage, long entries, int level, long actualDataSize, long footerLength, long uncompressedSize) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.SECTION_SIZE);
        long rolledTS = System.currentTimeMillis();

        bb.putInt(level);
        bb.putLong(entries);
        bb.putLong(actualDataSize);
        bb.putLong(footerLength);
        bb.putLong(rolledTS);
        bb.putLong(uncompressedSize);
        bb.flip();
        write(storage, COMPLETED_SECTION_START, bb);
        this.completed = new CompletedSection(level, entries, actualDataSize, footerLength, rolledTS, uncompressedSize);
    }

    public void writeDeleted(Storage storage) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.SECTION_SIZE);
        long deletedTS = System.currentTimeMillis();

        bb.putLong(deletedTS);
        bb.flip();
        write(storage, DELETED_SECTION_START, bb);
        this.deleted = new DeletedSection(deletedTS);
    }

    private static OpenSection readOpenSection(ByteBuffer bb, String expectedMagic) {
        ByteBuffer data = validateSection(bb, OPEN_SECTION_START);
        if (!data.hasRemaining()) {
            return null;
        }
        String magic = Serializers.VSTRING.fromBytes(data);
        long created = data.getLong();
        WriteMode writeMode = WriteMode.of(data.getInt());
        long fileSize = data.getLong();
        long dataSize = data.getLong();
        boolean encrypted = data.getInt() == 1;

        validateMagic(magic, expectedMagic);

        return new OpenSection(magic, created, writeMode, fileSize, dataSize, encrypted);
    }

    private static CompletedSection readCompleteSection(ByteBuffer bb) {
        ByteBuffer data = validateSection(bb, COMPLETED_SECTION_START);
        if (!data.hasRemaining()) {
            return null;
        }
        int level = data.getInt();
        long entries = data.getLong();
        long actualDataSize = data.getLong();
        long footerLength = data.getLong();
        long rolled = data.getLong();
        long uncompressedSize = data.getLong();


        return new CompletedSection(level, entries, actualDataSize, footerLength, rolled, uncompressedSize);
    }

    private static DeletedSection readDeletedSection(ByteBuffer bb) {
        ByteBuffer data = validateSection(bb, DELETED_SECTION_START);
        if (!data.hasRemaining()) {
            return null;
        }
        long timestamp = data.getLong();
        return new DeletedSection(timestamp);
    }

    private static ByteBuffer validateSection(ByteBuffer bb, int position) {
        bb.limit(position + SECTION_SIZE).position(position);
        int length = bb.getInt();
        if (length == 0) {
            return ByteBuffer.allocate(0);
        }
        if (length < 0 || length > SECTION_SIZE) {
            throw new CorruptedSegmentException("Invalid header length: " + length);
        }
        int checksum = bb.getInt();
        bb.limit(bb.position() + length);
        int computed = ByteBufferChecksum.crc32(bb);
        if (checksum != computed) {
            throw new CorruptedSegmentException("Checksum failed");
        }
        return bb;
    }

    private static void write(Storage storage, long position, ByteBuffer headerData) {
        try {
            int dataSize = headerData.remaining();

            if (position < 0 || position + dataSize >= BYTES) {
                throw new IllegalStateException("Invalid header positions / size. POS: " + position + ", SIZE: " + dataSize);
            }

            ByteBuffer withChecksumAndLength = ByteBuffer.allocate(SECTION_SIZE);

            if (SECTION_HEADER_BYTES + dataSize >= SECTION_SIZE) { //checksum
                throw new IllegalStateException("Header section data too large, max size: " + (SECTION_SIZE - SECTION_HEADER_BYTES));
            }
            withChecksumAndLength.putInt(dataSize);
            withChecksumAndLength.putInt(ByteBufferChecksum.crc32(headerData));
            withChecksumAndLength.put(headerData);
            withChecksumAndLength.position(0);//do not flip, the header will always have the fixed size

            long prevPos = storage.position();
            storage.position(position);
            int written = storage.write(withChecksumAndLength);
            if (written != LogHeader.SECTION_SIZE) {
                throw new IllegalStateException("Unexpected written header length");
            }
            storage.position(prevPos);
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }

    private static void validateMagic(String actualMagic, String expectedMagic) {
        byte[] actual = actualMagic.getBytes(StandardCharsets.UTF_8);
        byte[] expected = expectedMagic.getBytes(StandardCharsets.UTF_8);
        if (!Arrays.equals(expected, actual)) {
            throw new InvalidMagic(expectedMagic, actualMagic);
        }
    }

    @Override
    public String toString() {
        return "LogHeader{" +
                "type=" + type() +
                ", open=" + open +
                ", completed=" + completed +
                ", deleted=" + deleted +
                '}';
    }

}
