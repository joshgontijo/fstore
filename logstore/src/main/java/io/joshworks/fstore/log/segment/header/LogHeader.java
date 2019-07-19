package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.record.ByteBufferChecksum;
import io.joshworks.fstore.log.segment.CorruptedSegmentException;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.WriteMode;

import java.nio.ByteBuffer;
import java.util.Objects;

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
    private final Storage storage;

    private LogHeader(Storage storage) {
        this.storage = storage;
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

    public boolean readOnly() {
        Type type = type();
        return !Type.LOG_HEAD.equals(type) && !Type.MERGE_OUT.equals(type);
    }

    public long footerStart() {
        return BYTES + this.dataSize() + Log.EOL.length;
    }

    public long created() {
        return open == null ? UNKNOWN : open.created;
    }

    public long physicalSize() {
        return completed == null ? open.physical : completed.physical;
    }

    public long logicalSize() {
        if (completed != null) {
            return footerStart() + completed.footerLength;
        }
        if (open != null) {
            return open.dataSize;
        }
        throw new IllegalStateException("Unknown segment state");
    }

    public long dataSize() {
        return open.dataSize;
    }

    public long actualDataSize() {
        return completed == null ? 0 : completed.actualDataLength;
    }

    public long uncompressedDataSize() {
        return completed == null ? 0 : completed.uncompressedSize;
    }

    public long headerSize() {
        return LogHeader.BYTES;
    }

    public long footerSize() {
        if (completed == null) {
            return 0;
        }
        return completed.footerLength;
    }

    public boolean encrypted() {
        return open != null && open.encrypted;
    }

    public long entries() {
        return completed == null ? 0 : completed.entries;
    }

    public int level() {
        if (open == null) {
            throw new IllegalStateException("Unknown segment state");
        }
        return completed == null ? 0 : completed.level;
    }



    public long rolled() {
        return completed == null ? UNKNOWN : completed.rolled;
    }

    public long uncompressedSize() {
        return completed == null ? UNKNOWN : completed.uncompressedSize;
    }

    public long maxDataPosition() {
        return Log.START + dataSize();
    }

    public long logEnd() {
        return footerStart() + footerSize();
    }

    public long footerMapPosition() {
        if (completed == null) {
            throw new IllegalStateException("Can only read footer map of read only segment");
        }
        return completed.footerMapPosition;
    }

    public static LogHeader read(Storage storage) {
        if (storage.position() < BYTES) {
            storage.position(BYTES);
        }
        LogHeader header = new LogHeader(storage);
        ByteBuffer bb = readHeader(storage);
        if (bb.remaining() == 0) {
            return header;
        }
        header.open = readOpenSection(bb);
        header.completed = readCompleteSection(bb);
        header.deleted = readDeletedSection(bb);

        return header;
    }

    private static ByteBuffer readHeader(Storage storage) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.BYTES);
        storage.read(HEADER_START, bb);
        return bb.flip();
    }

    public void writeNew(WriteMode mode, long fileSize, long dataSize, boolean encrypted) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.SECTION_SIZE);
        long created = System.currentTimeMillis();

        bb.putLong(created);
        bb.putInt(mode.val);
        bb.putLong(fileSize);
        bb.putLong(dataSize);
        bb.putInt(encrypted ? 1 : 0);
        bb.flip();
        write(OPEN_SECTION_START, bb);
        this.open = new OpenSection(created, mode, fileSize, dataSize, encrypted);
        verifyWrite();
    }

    public void writeCompleted(long entries, int level, long actualDataSize, long footerMapPosition, long footerLength, long uncompressedSize, long physical) {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.SECTION_SIZE);
        long rolledTS = System.currentTimeMillis();

        bb.putInt(level);
        bb.putLong(entries);
        bb.putLong(actualDataSize);
        bb.putLong(footerMapPosition);
        bb.putLong(footerLength);
        bb.putLong(rolledTS);
        bb.putLong(uncompressedSize);
        bb.putLong(physical);
        bb.flip();
        write(COMPLETED_SECTION_START, bb);
        this.completed = new CompletedSection(level, entries, actualDataSize, footerMapPosition, footerLength, rolledTS, uncompressedSize, physical);
        verifyWrite();
    }

    private void verifyWrite() {
        LogHeader readHeader = read(storage);
        if (!this.equals(readHeader)) {
            throw new IllegalStateException("Failed to write header, expected: " + this + " got: " + readHeader);
        }
    }

    public void writeDeleted() {
        ByteBuffer bb = ByteBuffer.allocate(LogHeader.SECTION_SIZE);
        long deletedTS = System.currentTimeMillis();

        bb.putLong(deletedTS);
        bb.flip();
        write(DELETED_SECTION_START, bb);
        this.deleted = new DeletedSection(deletedTS);
        verifyWrite();
    }

    private static OpenSection readOpenSection(ByteBuffer bb) {
        ByteBuffer data = validateSection(bb, OPEN_SECTION_START);
        if (!data.hasRemaining()) {
            return null;
        }
        long created = data.getLong();
        WriteMode writeMode = WriteMode.of(data.getInt());
        long fileSize = data.getLong();
        long dataSize = data.getLong();
        boolean encrypted = data.getInt() == 1;

        return new OpenSection(created, writeMode, fileSize, dataSize, encrypted);
    }

    private static CompletedSection readCompleteSection(ByteBuffer bb) {
        ByteBuffer data = validateSection(bb, COMPLETED_SECTION_START);
        if (!data.hasRemaining()) {
            return null;
        }
        int level = data.getInt();
        long entries = data.getLong();
        long actualDataSize = data.getLong();
        long footerMapPosition = data.getLong();
        long footerLength = data.getLong();
        long rolled = data.getLong();
        long uncompressedSize = data.getLong();
        long physical = data.getLong();


        return new CompletedSection(level, entries, actualDataSize, footerMapPosition, footerLength, rolled, uncompressedSize, physical);
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

    private void write(long position, ByteBuffer headerData) {
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

            int written = storage.write(position, withChecksumAndLength);
            if (written != LogHeader.SECTION_SIZE) {
                throw new IllegalStateException("Unexpected written header length");
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write header", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogHeader logHeader = (LogHeader) o;
        return Objects.equals(open, logHeader.open) &&
                Objects.equals(completed, logHeader.completed) &&
                Objects.equals(deleted, logHeader.deleted);
    }

    @Override
    public int hashCode() {
        return Objects.hash(open, completed, deleted);
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
