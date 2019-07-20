package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;

import static io.joshworks.fstore.log.record.RecordHeader.LENGTH_SIZE;
import static java.util.Objects.requireNonNull;

public class DataStream {

    private final Storage storage;

    private final Reader forwardReader;
    private final BulkReader bulkForwardReader;
    private final BulkReader bulkBackwardReader;
    private final Reader backwardReader;

    private final ThreadLocal<Record> localRecord = ThreadLocal.withInitial(Record::new);
    private final BufferPool bufferPool;

    //maxEntryLength
    private final long maxAllowedEntrySize;

    public DataStream(BufferPool bufferPool, Storage storage) {
        this(bufferPool, storage, 0.1, Size.KB.ofInt(16), Long.MAX_VALUE);
    }

    public DataStream(BufferPool bufferPool, Storage storage, double checksumProb, int readPageSize) {
        this(bufferPool, storage, checksumProb, readPageSize, Long.MAX_VALUE);
    }

    public DataStream(BufferPool bufferPool, Storage storage, double checksumProb, int readPageSize, long maxAllowedEntrySize) {
        requireNonNull(bufferPool, "BufferPool must be provided");
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0 and 1");
        }
        if (maxAllowedEntrySize <= 0) {
            throw new IllegalArgumentException("maxAllowedEntrySize must be greater than zero");
        }
        this.bufferPool = bufferPool;
        this.maxAllowedEntrySize = maxAllowedEntrySize;
        this.storage = storage;
        checksumProb = (int) (checksumProb * 100);
        this.forwardReader = new ForwardRecordReader(bufferPool, checksumProb, readPageSize);
        this.backwardReader = new BackwardRecordReader(bufferPool, checksumProb, readPageSize);
        this.bulkForwardReader = new BulkForwardRecordReader(bufferPool, checksumProb, readPageSize);
        this.bulkBackwardReader = new BulkBackwardRecordReader(bufferPool, checksumProb, readPageSize);
    }

    public <T> long write(long position, T entry, Serializer<T> serializer) {
        return write(position, entry, serializer, Integer.MAX_VALUE);
    }

    public <T> long write(long position, T entry, Serializer<T> serializer, long maxEntrySize) {
        ByteBuffer writeBuffer = null;
        try {
            writeBuffer = writeToBuffer(entry, serializer, maxEntrySize);
            if (writeBuffer == null || writeBuffer.remaining() > maxEntrySize) {
                return Storage.EOF;
            }
            long recordLen = writeBuffer.remaining();
            long written = storage.write(position, writeBuffer);
            checkWrittenBytes(recordLen, written);

            return position;
        } finally {
            if (writeBuffer != null) {
                bufferPool.free(writeBuffer);
            }
        }
    }

    public <T> long write(T entry, Serializer<T> serializer) {
        return write(entry, serializer, Integer.MAX_VALUE);
    }

    public <T> long write(T entry, Serializer<T> serializer, long maxEntrySize) {
        ByteBuffer writeBuffer = null;
        try {
            writeBuffer = writeToBuffer(entry, serializer, maxEntrySize);
            if (writeBuffer == null || writeBuffer.remaining() > maxEntrySize) {
                return Storage.EOF;
            }
            long recordLen = writeBuffer.remaining();
            long storagePos = storage.position();
            long written = storage.write(writeBuffer);
            checkWrittenBytes(recordLen, written);

            return storagePos;
        } finally {
            if (writeBuffer != null) {
                bufferPool.free(writeBuffer);
            }
        }
    }

    private <T> ByteBuffer writeToBuffer(T entry, Serializer<T> serializer, long maxEntrySize) {
        ByteBuffer bb = bufferPool.allocate();
        int bufferSize = bb.remaining();
        try {
            bb.position(RecordHeader.MAIN_HEADER);
            serializer.writeTo(entry, bb);
            int len = bb.position() - RecordHeader.MAIN_HEADER;
            bb.position(RecordHeader.MAIN_HEADER);
            bb.limit(RecordHeader.MAIN_HEADER + len);
            int checksum = ByteBufferChecksum.crc32(bb);
            bb.putInt(0, len);
            bb.putInt(LENGTH_SIZE, checksum);
            bb.limit(RecordHeader.MAIN_HEADER + len + RecordHeader.SECONDARY_HEADER);
            bb.putInt(RecordHeader.MAIN_HEADER + len, len);
            bb.position(0);
        } catch (BufferOverflowException e) {
            //max allowed size, if greater than an exception must be thrown
            //The main usage of this is because Segment#append needs to validate entry against the total log size
            int capacity = bb.capacity();
            if (capacity > maxAllowedEntrySize) {
                bufferPool.free(bb);
                throw new RuntimeException("Entry of size: " + capacity + " is greater than allowed size of: " + maxAllowedEntrySize);
            }
            if (capacity > maxEntrySize) {
                bufferPool.free(bb);
                return null;
            }
            bufferPool.free(bb);
            bufferPool.resize(bufferSize * 2);
            return writeToBuffer(entry, serializer, maxEntrySize);
        } catch (Exception e) {
            bufferPool.free(bb);
            throw new RuntimeException("Failed to write to buffer", e);
        }
        return bb;
    }


    public long write(ByteBuffer entry) {
        long storagePos = storage.position();
        try (Record record = localRecord.get()) {
            ByteBuffer[] buffers = record.create(entry);
            long length = record.length();
            long written = storage.write(buffers);
            checkWrittenBytes(length, written);
            return storagePos;
        }
    }

    public long write(ByteBuffer[] entries) {
        long storagePos = storage.position();
        Record.Records records = Record.create(entries);
        long written = storage.write(records.buffers);
        checkWrittenBytes(records.totalLength, written);
        return storagePos;
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

    public void position(long position) {
        storage.position(position);
    }

    public long position() {
        return storage.position();
    }

    public long length() {
        return storage.length();
    }

    private void checkWrittenBytes(long expected, long written) {
        if (written != expected) {
            throw new IllegalStateException("Expected write of size: " + expected + " actual bytes written: " + written);
        }
    }

}
