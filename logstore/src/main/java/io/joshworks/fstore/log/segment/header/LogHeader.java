package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.util.Objects;

public class LogHeader {

    public static final int BYTES = 4096;
    public static final int SECTION_SIZE = 512; //data + checksum (effective available space SECTION_SIZE - 4bytes (checksum)

    public static final int OPEN_SECTION_START = 0;
    public static final int COMPLETED_SECTION_START = OPEN_SECTION_START + SECTION_SIZE;
    public static final int MERGE_SECTION_START = COMPLETED_SECTION_START + SECTION_SIZE;
    public static final int DELETED_SECTION_START = MERGE_SECTION_START + SECTION_SIZE;

    public static final int UNKNOWN = -1;

    private OpenSection open;
    private CompletedSection completed;
    private DeletedSection deleted;
    private final DataStream stream;

    private final Serializer<OpenSection> openSerializer = KryoStoreSerializer.of(OpenSection.class);
    private final Serializer<CompletedSection> completedSerizalizer = KryoStoreSerializer.of(CompletedSection.class);
    private final Serializer<DeletedSection> deletedSerializer = KryoStoreSerializer.of(DeletedSection.class);
    private final Serializer<MergeSection> mergeSerializer = KryoStoreSerializer.of(MergeSection.class);

    private LogHeader(DataStream stream) {
        this.stream = stream;
        if (stream.position() < BYTES) {
            stream.position(BYTES);
        }
        this.open = stream.read(Direction.FORWARD, OPEN_SECTION_START, openSerializer).entry();
        this.completed = stream.read(Direction.FORWARD, COMPLETED_SECTION_START, completedSerizalizer).entry();
        this.deleted = stream.read(Direction.FORWARD, DELETED_SECTION_START, deletedSerializer).entry();
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
        if (completed == null) {
            throw new IllegalStateException("Segment is not read only");
        }
        return completed.footerStart;
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
        return dataSize();
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

    public long uncompressedSize() {
        return completed == null ? UNKNOWN : completed.uncompressedSize;
    }

    public long maxDataPosition() {
        if (completed != null) {
            return Log.START + completed.actualDataLength;
        }
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

    public static LogHeader read(DataStream stream) {
        return new LogHeader(stream);
    }

    public void writeNew(WriteMode mode, long fileSize, long dataSize, boolean encrypted) {
        this.open = new OpenSection(System.currentTimeMillis(), mode, fileSize, dataSize, encrypted);
        stream.write(OPEN_SECTION_START, open, openSerializer);
        verifyWrite();
    }

    public void writeCompleted(long entries, int level, long actualDataSize, long footerMapPosition, long footerStart, long footerLength, long uncompressedSize, long physical) {
        this.completed = new CompletedSection(level, entries, actualDataSize, footerMapPosition, footerStart, footerLength, System.currentTimeMillis(), uncompressedSize, physical);
        stream.write(COMPLETED_SECTION_START, completed, completedSerizalizer);
        verifyWrite();
    }

    private void verifyWrite() {
        LogHeader readHeader = new LogHeader(stream);
        if (!this.equals(readHeader)) {
            throw new IllegalStateException("Failed to write header, expected: " + this + " got: " + readHeader);
        }
    }

    public void writeDeleted() {
        this.deleted = new DeletedSection(System.currentTimeMillis());
        stream.write(DELETED_SECTION_START, deleted, deletedSerializer);
        verifyWrite();
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
