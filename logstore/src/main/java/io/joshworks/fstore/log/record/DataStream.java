package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

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
    private final BufferPool bufferPool;

    private final ThreadLocal<ByteBuffer[]> record = ThreadLocal.withInitial(() -> new ByteBuffer[]{ByteBuffer.allocate(RecordHeader.MAIN_HEADER), null, ByteBuffer.allocate(RecordHeader.SECONDARY_HEADER)});

    public DataStream(BufferPool bufferPool, double checksumProb, int maxEntrySize, int readPageSize) {
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }
        checksumProb = (int) (checksumProb * 100);
        this.maxEntrySize = maxEntrySize;
        this.bufferPool = Objects.requireNonNull(bufferPool, "BufferPool must be provided");
        this.forwardReader = new ForwardRecordReader(checksumProb, maxEntrySize, readPageSize);
        this.backwardReader = new BackwardRecordReader(checksumProb, maxEntrySize, readPageSize);
        this.bulkForwardReader = new BulkForwardRecordReader(checksumProb, maxEntrySize, readPageSize);
        this.bulkBackwardReader = new BulkBackwardRecordReader(checksumProb, maxEntrySize, readPageSize);
    }

    @Override
    public long write(Storage storage, ByteBuffer entryData) {
        long storagePos = storage.position();
        validateStoragePosition(storagePos);

        int entrySize = entryData.remaining();
        int recordSize = RecordHeader.HEADER_OVERHEAD + entrySize;

        checkRecordSize(recordSize);


        ByteBuffer[] buffers = record.get();
        try {
            buffers[0].putInt(entrySize);
            buffers[0].putInt(ByteBufferChecksum.crc32(entryData));
            buffers[1] = entryData;
            buffers[2].putInt(entrySize);

            storage.write(buffers);
            return storagePos;

        } finally {
            buffers[0].clear();
            buffers[1] = null;
            buffers[2].clear();
        }
    }

    private void checkRecordSize(int recordSize) {
        if (recordSize > maxEntrySize) {
            throw new IllegalArgumentException("Record cannot exceed " + maxEntrySize + " bytes");
        }
    }

    @Override
    public <T> long write(Storage storage, T data, Serializer<T> serializer) {
        long storagePos = storage.position();
        validateStoragePosition(storagePos);

        ByteBuffer writeBuffer = bufferPool.allocate(Memory.PAGE_SIZE);
        writeBuffer.clear();//set limit to capacity, used to avoid havin BufferOverflow for entry > PAGE_SIZE

        try {
            write(data, serializer, writeBuffer);
        } catch (BufferOverflowException boe) {
            int bufferCapacity = writeBuffer.capacity();
            bufferPool.free(writeBuffer);
            if (bufferCapacity > maxEntrySize) {
                throw new IllegalArgumentException("Record cannot exceed " + maxEntrySize + " bytes");
            }
            int entrySize = serializer.toBytes(data).limit() + RecordHeader.HEADER_OVERHEAD;
            writeBuffer = bufferPool.allocate(entrySize);
            write(data, serializer, writeBuffer);
        }
        try {
            checkRecordSize(writeBuffer.capacity());
            int written = storage.write(writeBuffer);
            if (written == Storage.EOF) {
                return Storage.EOF;
            }
            return storagePos;
        } finally {
            bufferPool.free(writeBuffer);
        }
    }

    private <T> void write(T data, Serializer<T> serializer, ByteBuffer buffer) {
        buffer.limit(buffer.capacity());
        buffer.position(RecordHeader.MAIN_HEADER);

        //data
        serializer.writeTo(data, buffer);

        int entrySize = buffer.position() - RecordHeader.MAIN_HEADER;

        //extract checksum
        buffer.limit(buffer.position());
        buffer.position(RecordHeader.MAIN_HEADER);
        int checksum = ByteBufferChecksum.crc32(buffer);

        //secondary header
        buffer.limit(buffer.limit() + RecordHeader.SECONDARY_HEADER);
        buffer.position(RecordHeader.MAIN_HEADER + entrySize);
        buffer.putInt(entrySize); //secondary length

        //main header
        buffer.position(0);
        buffer.putInt(entrySize); //length
        buffer.putInt(checksum); //checksum
        buffer.position(0);
        buffer.limit(RecordHeader.HEADER_OVERHEAD + entrySize);
    }

    private void validateStoragePosition(long storagePos) {
        if (storagePos < Log.START) {
            throw new IllegalStateException("storage position is less than " + Log.START);
        }
    }

    @Override
    public <T> RecordEntry<T> read(Storage storage, Direction direction, long position, Serializer<T> serializer) {
        try {
            Reader reader = Direction.FORWARD.equals(direction) ? forwardReader : backwardReader;
            return reader.read(storage, bufferPool, position, serializer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

    @Override
    public <T> List<RecordEntry<T>> bulkRead(Storage storage, Direction direction, long position, Serializer<T> serializer) {
        try {
            BulkReader reader = Direction.FORWARD.equals(direction) ? bulkForwardReader : bulkBackwardReader;
            return reader.read(storage, bufferPool, position, serializer);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

}
