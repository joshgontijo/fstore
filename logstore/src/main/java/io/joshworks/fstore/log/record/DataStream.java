package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
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

    private final ThreadLocalBufferPool bufferPool;


    public DataStream(ThreadLocalBufferPool bufferPool, Storage storage) {
        this(bufferPool, storage, 0.1, Size.KB.ofInt(16));
    }

    public DataStream(ThreadLocalBufferPool bufferPool, Storage storage, double checksumProb, int readPageSize) {
        requireNonNull(bufferPool, "BufferPool must be provided");
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0 and 1");
        }
        this.bufferPool = bufferPool;
        this.storage = storage;
        checksumProb = (int) (checksumProb * 100);
        this.forwardReader = new ForwardRecordReader(bufferPool, checksumProb, readPageSize);
        this.backwardReader = new BackwardRecordReader(bufferPool, checksumProb, readPageSize);
        this.bulkForwardReader = new BulkForwardRecordReader(bufferPool, checksumProb, readPageSize);
        this.bulkBackwardReader = new BulkBackwardRecordReader(bufferPool, checksumProb, readPageSize);
    }

    public <T> long write(long position, T entry, Serializer<T> serializer) {
        try (bufferPool) {
            ByteBuffer dst = bufferPool.allocate();
            serialize(entry, serializer, dst);
            long recordLen = dst.remaining();
            long written = storage.write(position, dst);
            checkWrittenBytes(recordLen, written);
            return position;
        }
    }

    public <T> long write(T entry, Serializer<T> serializer) {
        try (bufferPool) {
            ByteBuffer dst = bufferPool.allocate();
            serialize(entry, serializer, dst);
            long recordLen = dst.remaining();
            long storagePos = storage.position();
            long written = storage.write(dst);
            checkWrittenBytes(recordLen, written);
            return storagePos;
        }
    }

    private <T> void serialize(T entry, Serializer<T> serializer, ByteBuffer dst) {
        try {
            dst.position(RecordHeader.MAIN_HEADER);
            serializer.writeTo(entry, dst);
            int len = dst.position() - RecordHeader.MAIN_HEADER;
            if (dst.remaining() < RecordHeader.SECONDARY_HEADER) {
                throw new IllegalArgumentException("Record too large: Max allowed size is " + (dst.capacity() - RecordHeader.HEADER_OVERHEAD));
            }
            dst.position(RecordHeader.MAIN_HEADER);
            dst.limit(RecordHeader.MAIN_HEADER + len);
            int checksum = ByteBufferChecksum.crc32(dst);
            dst.putInt(0, len);
            dst.putInt(LENGTH_SIZE, checksum);
            dst.limit(RecordHeader.MAIN_HEADER + len + RecordHeader.SECONDARY_HEADER);
            dst.putInt(RecordHeader.MAIN_HEADER + len, len);
            dst.position(0);
        } catch (BufferOverflowException e) {
            throw new IllegalArgumentException("Record too large: Max allowed size is " + dst.capacity());
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
