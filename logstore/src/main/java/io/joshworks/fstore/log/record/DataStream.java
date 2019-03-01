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

//THREAD SAFE
public class DataStream implements IDataStream {

    public static final int MAX_BULK_READ_RESULT = 100;

    static final int READ_BUFFER_SIZE = Memory.PAGE_SIZE;
    static final int BULK_READ_BUFFER_SIZE = Memory.PAGE_SIZE * 2;

    //hard limit is required to avoid memory issues in case of broken record

    private final double checksumProb;
    private final int maxEntrySize;

    private final Reader forwardReader;
    private final BulkReader bulkForwardReader;
    private final BulkReader bulkBackwardReader;
    private final Reader backwardReader;
    private final BufferPool bufferPool;

    public DataStream(BufferPool bufferPool, double checksumProb, int maxEntrySize) {
        this.checksumProb = (int) (checksumProb * 100);
        this.maxEntrySize = maxEntrySize;
        this.bufferPool = Objects.requireNonNull(bufferPool, "BufferPool must be provided");
        this.forwardReader = new ForwardRecordReader(checksumProb, maxEntrySize, READ_BUFFER_SIZE);
        this.backwardReader = new BackwardRecordReader(checksumProb, maxEntrySize, READ_BUFFER_SIZE);
        this.bulkForwardReader = new BulkForwardRecordReader(checksumProb, maxEntrySize, BULK_READ_BUFFER_SIZE);
        this.bulkBackwardReader = new BulkBackwardRecordReader(checksumProb, maxEntrySize, BULK_READ_BUFFER_SIZE);
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }
    }

    @Override
    public long write(Storage storage, ByteBuffer bytes) {
        long storagePos = storage.writePosition();
        validateStoragePosition(storagePos);

        int recordSize = RecordHeader.HEADER_OVERHEAD + bytes.remaining();

        checkRecordSize(recordSize);

        ByteBuffer bb = bufferPool.allocate(recordSize);
        try {
            int entrySize = bytes.remaining();
            bb.putInt(entrySize);
            bb.putInt(Checksum.crc32(bytes));
            bb.put(bytes);
            bb.putInt(entrySize);

            bb.flip();
            int written = storage.write(bb);
            if (written == Storage.EOF) {
                return Storage.EOF;
            }
            return storagePos;

        } finally {
            bufferPool.free(bb);
        }
    }

    private void checkRecordSize(int recordSize) {
        if (recordSize > maxEntrySize) {
            throw new IllegalArgumentException("Record cannot exceed " + maxEntrySize + " bytes");
        }
    }

    @Override
    public <T> long write(Storage storage, T data, Serializer<T> serializer) {
        long storagePos = storage.writePosition();
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
        int checksum = Checksum.crc32(buffer);

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
