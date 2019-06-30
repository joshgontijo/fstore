package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.log.Direction;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Class responsible for handling record headers
 */
public class DataStream implements IDataStream {

    public static final int MAX_BULK_READ_RESULT = 100;

    //hard limit is required to avoid memory issues in case of broken record

    private final int maxEntrySize;

    private final Reader forwardReader;
    private final BulkReader bulkForwardReader;
    private final BulkReader bulkBackwardReader;
    private final Reader backwardReader;

    private final ThreadLocal<Record> localRecord = ThreadLocal.withInitial(Record::new);

    public DataStream(BufferPool bufferPool, double checksumProb, int maxEntrySize, int readPageSize) {
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0 and 1");
        }
        checksumProb = (int) (checksumProb * 100);
        this.maxEntrySize = maxEntrySize;
        requireNonNull(bufferPool, "BufferPool must be provided");
        this.forwardReader = new ForwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
        this.backwardReader = new BackwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
        this.bulkForwardReader = new BulkForwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
        this.bulkBackwardReader = new BulkBackwardRecordReader(bufferPool, checksumProb, maxEntrySize, readPageSize);
    }

    @Override
    public long write(Storage storage, ByteBuffer entryData) {
        long storagePos = storage.position();
        try (Record record = localRecord.get()) {
            checkRecordSize(record.size());
            ByteBuffer[] buffers = record.create(entryData);
            storage.write(buffers);
            return storagePos;
        }
    }

    //useful for batch inserting
    public long write(Storage storage, ByteBuffer[] entries) {
        long storagePos = storage.position();
        try (Record record = localRecord.get()) {
            checkRecordSize(record.size());
            ByteBuffer[] records = Record.create(entries);
            storage.write(records);
            return storagePos;
        }
    }

    private void checkRecordSize(long recordSize) {
        if (recordSize > maxEntrySize) {
            throw new IllegalArgumentException("Record cannot exceed " + maxEntrySize + " bytes");
        }
    }

    @Override
    public <T> RecordEntry<T> read(Storage storage, Direction direction, long position, Serializer<T> serializer) {
        try {
            Reader reader = Direction.FORWARD.equals(direction) ? forwardReader : backwardReader;
            return reader.read(storage, position, serializer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

    @Override
    public <T> List<RecordEntry<T>> bulkRead(Storage storage, Direction direction, long position, Serializer<T> serializer) {
        try {
            BulkReader reader = Direction.FORWARD.equals(direction) ? bulkForwardReader : bulkBackwardReader;
            return reader.read(storage, position, serializer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

}
