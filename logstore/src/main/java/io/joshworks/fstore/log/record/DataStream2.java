package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DataStream2 {

    private final Storage storage;
    private final int maxEntrySize;

    private final Reader forwardReader;
    private final BulkReader bulkForwardReader;
    private final BulkReader bulkBackwardReader;
    private final Reader backwardReader;

    private final ThreadLocal<Record> localRecord = ThreadLocal.withInitial(Record::new);

    public DataStream2(BufferPool bufferPool, Storage storage) {
        this(bufferPool, storage, Size.MB.intOf(5), 0.1, Size.KB.intOf(16));
    }

    public DataStream2(BufferPool bufferPool, Storage storage, int maxEntrySize, double checksumProb, int readPageSize) {
        requireNonNull(bufferPool, "BufferPool must be provided");
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0 and 1");
        }
        this.storage = storage;
        checksumProb = (int) (checksumProb * 100);
        this.maxEntrySize = maxEntrySize;
        this.forwardReader = new ForwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
        this.backwardReader = new BackwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
        this.bulkForwardReader = new BulkForwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
        this.bulkBackwardReader = new BulkBackwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
    }

    public long write(ByteBuffer entry) {
        long storagePos = storage.position();
        try (Record record = localRecord.get()) {
            ByteBuffer[] buffers = record.create(entry);
            long length = record.length();
            checkRecordSize(record.length());
            long written = storage.write(buffers);
            checkWrittenBytes(length, written);
            return storagePos;
        }
    }

    public long write(ByteBuffer[] entries) {
        long storagePos = storage.position();
        try (Record record = localRecord.get()) {
            Record.Records records = Record.create(entries);
            checkRecordSize(record.length());
            long written = storage.write(records.buffers);
            checkWrittenBytes(records.totalLength, written);
            return storagePos;
        }
    }

    public <T> RecordEntry<T> read(Direction direction, long position, Serializer<T> serializer) {
        try {
            Reader reader = Direction.FORWARD.equals(direction) ? forwardReader : backwardReader;
            return reader.read(storage, position, serializer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

    public <T> List<RecordEntry<T>> bulkRead(Direction direction, long position, Serializer<T> serializer) {
        try {
            BulkReader reader = Direction.FORWARD.equals(direction) ? bulkForwardReader : bulkBackwardReader;
            return reader.read(storage, position, serializer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

    private void checkRecordSize(long recordSize) {
        if (recordSize > maxEntrySize) {
            throw new IllegalArgumentException("Record cannot exceed " + maxEntrySize + " bytes");
        }
    }

    private void checkWrittenBytes(long expected, long written) {
        if (written != expected) {
            throw new IllegalStateException("Expected write of size: " + expected + " actual bytes written: " + written);
        }
    }
}
