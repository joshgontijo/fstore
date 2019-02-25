package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

//THREAD SAFE
public class DataStream implements IDataStream {

    public static final int MAX_BULK_READ_RESULT = 100;

    static final int READ_BUFFER_SIZE = Memory.PAGE_SIZE;
    static final int BULK_READ_BUFFER_SIZE = Memory.PAGE_SIZE * 2;

    //hard limit is required to avoid memory issues in case of broken record

    private final double checksumProb;
    private final int maxEntrySize;
    private final ThreadLocalRandom rand = ThreadLocalRandom.current();
    private final RecordReader forwardReader = new ForwardRecordReader();
    private final RecordReader bulkForwardReader = new BulkForwardRecordReader();
    private final RecordReader bulkBackwardReader = new BulkBackwardRecordReader();
    private final RecordReader backwardReader = new BackwardRecordReader();
    private final BufferPool bufferPool;

    public DataStream(BufferPool bufferPool, double checksumProb, int maxEntrySize) {
        this.checksumProb = (int) (checksumProb * 100);
        this.maxEntrySize = maxEntrySize;
        this.bufferPool = Objects.requireNonNull(bufferPool, "BufferPool must be provided");
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
    public BufferRef read(Storage storage, Direction direction, long position) {
        try {
            RecordReader reader = Direction.FORWARD.equals(direction) ? forwardReader : backwardReader;
            return reader.read(storage, bufferPool, position);

        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

    @Override
    public BufferRef bulkRead(Storage storage, Direction direction, long position) {
        try {
            RecordReader reader = Direction.FORWARD.equals(direction) ? bulkForwardReader : bulkBackwardReader;
            return reader.read(storage, bufferPool, position);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read at position " + position, e);
        }
    }

    private void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (checksumProb >= 100 && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
        if (rand.nextInt(100) < checksumProb && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }

    private final class ForwardRecordReader implements RecordReader {

        @Override
        public BufferRef read(Storage storage, BufferPool bufferPool, long position) {
            ByteBuffer buffer = bufferPool.allocate(READ_BUFFER_SIZE);
            try {
                storage.read(position, buffer);
                buffer.flip();

                if (buffer.remaining() < RecordHeader.MAIN_HEADER) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int length = buffer.getInt();
                checkRecordLength(length, position);
                if (length == 0) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int recordSize = length + RecordHeader.HEADER_OVERHEAD;
                if (recordSize > buffer.limit()) {
                    bufferPool.free(buffer);
                    buffer = bufferPool.allocate(recordSize);
                    storage.read(position, buffer);
                    buffer.flip();
                    buffer.getInt(); //skip length
                }

                int checksum = buffer.getInt();
                buffer.limit(buffer.position() + length);
                checksum(checksum, buffer, position);
                return BufferRef.of(buffer, bufferPool);
            } finally {
                bufferPool.free(buffer);
            }
        }
    }

    private final class BulkForwardRecordReader implements RecordReader {

        @Override
        public BufferRef read(Storage storage, BufferPool bufferPool, long position) {
            ByteBuffer buffer = bufferPool.allocate(BULK_READ_BUFFER_SIZE);
            try {
                storage.read(position, buffer);
                buffer.flip();

                if (buffer.remaining() < RecordHeader.MAIN_HEADER) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int length = buffer.getInt();
                checkRecordLength(length, position);
                if (length == 0) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int recordSize = length + RecordHeader.HEADER_OVERHEAD;
                if (recordSize > buffer.limit()) {
                    bufferPool.free(buffer);
                    buffer = bufferPool.allocate(recordSize);
                    storage.read(position, buffer);
                    buffer.flip();
                    buffer.getInt(); //skip length
                }

                buffer.position(buffer.position() - Integer.BYTES);
                int[] markers = new int[MAX_BULK_READ_RESULT];
                int[] lengths = new int[MAX_BULK_READ_RESULT];
                int i = 0;
                int originalLimit = buffer.limit();
                while (buffer.hasRemaining() && buffer.remaining() > RecordHeader.MAIN_HEADER && i < MAX_BULK_READ_RESULT) {
                    int pos = buffer.position();
                    int len = buffer.getInt();
                    checkRecordLength(len, position);
                    if (len == 0) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    if (buffer.remaining() < len + RecordHeader.CHECKSUM_SIZE) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    markers[i] = pos + RecordHeader.MAIN_HEADER;
                    lengths[i] = len;

                    int checksum = buffer.getInt();
                    buffer.limit(buffer.position() + len);
                    checksum(checksum, buffer, position + pos);
                    buffer.limit(originalLimit);

                    i++;
                    int newPos = buffer.position() + len + RecordHeader.SECONDARY_HEADER;
                    if (newPos > buffer.limit()) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    buffer.position(newPos);
                }
                return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
            } finally {
                bufferPool.free(buffer);
            }
        }
    }

    private final class BulkBackwardRecordReader implements RecordReader {

        @Override
        public BufferRef read(Storage storage, BufferPool bufferPool, long position) {
            ByteBuffer buffer = bufferPool.allocate(BULK_READ_BUFFER_SIZE);
            try {

                int limit = buffer.limit();
                if (position - limit < Log.START) {
                    int available = (int) (position - Log.START);
                    if (available == 0) {
                        return BufferRef.ofEmpty(buffer, bufferPool);
                    }
                    buffer.limit(available);
                    limit = available;
                }

                storage.read(position - limit, buffer);
                buffer.flip();
                if (buffer.remaining() == 0) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int recordDataEnd = buffer.limit() - RecordHeader.SECONDARY_HEADER;
                int length = buffer.getInt(recordDataEnd);
                checkRecordLength(length, position);
                if (length == 0) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int recordSize = length + RecordHeader.HEADER_OVERHEAD;

                if (recordSize > buffer.limit()) {
                    bufferPool.free(buffer);
                    buffer = bufferPool.allocate(recordSize);

                    buffer.limit(recordSize); //limit to the entry size, excluding the secondary header
                    long readStart = position - recordSize;
                    storage.read(readStart, buffer);
                    buffer.flip();
                }

                int[] markers = new int[MAX_BULK_READ_RESULT];
                int[] lengths = new int[MAX_BULK_READ_RESULT];
                int i = 0;
                int originalLimit = buffer.limit();
                while (i < MAX_BULK_READ_RESULT) {

                    buffer.limit(originalLimit);
                    buffer.position(originalLimit);

                    int len = buffer.getInt(originalLimit - RecordHeader.SECONDARY_HEADER);
                    int recordStart = originalLimit - len - RecordHeader.HEADER_OVERHEAD;
                    if (recordStart < 0) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    buffer.position(originalLimit - len - RecordHeader.HEADER_OVERHEAD);
                    checkRecordLength(length, position);

                    int pos = buffer.position();
                    if (len == 0) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    if (buffer.remaining() < len + RecordHeader.CHECKSUM_SIZE) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    checkRecordLength(len, pos);

                    if (buffer.remaining() < len + RecordHeader.CHECKSUM_SIZE) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }

                    markers[i] = pos + RecordHeader.MAIN_HEADER;
                    lengths[i] = len;

                    buffer.getInt(); //skip length
                    int checksum = buffer.getInt();
                    buffer.limit(buffer.position() + len);
                    checksum(checksum, buffer, position + pos);

                    originalLimit = pos;
                    i++;

                    if (originalLimit - RecordHeader.SECONDARY_HEADER <= 0) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                }

                return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
            } finally {
                bufferPool.free(buffer);
            }


        }
    }

    private final class BackwardRecordReader implements RecordReader {

        @Override
        public BufferRef read(Storage storage, BufferPool bufferPool, long position) {
            ByteBuffer buffer = bufferPool.allocate(READ_BUFFER_SIZE);
            try {
                int limit = buffer.limit();
                if (position - limit < Log.START) {
                    int available = (int) (position - Log.START);
                    if (available == 0) {
                        return BufferRef.ofEmpty(buffer, bufferPool);
                    }
                    buffer.limit(available);
                    limit = available;
                }

                storage.read(position - limit, buffer);
                buffer.flip();
                if (buffer.remaining() == 0) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int recordDataEnd = buffer.limit() - RecordHeader.SECONDARY_HEADER;
                int length = buffer.getInt(recordDataEnd);
                checkRecordLength(length, position);
                if (length == 0) {
                    return BufferRef.ofEmpty(buffer, bufferPool);
                }

                int recordSize = length + RecordHeader.HEADER_OVERHEAD;

                if (recordSize > buffer.limit()) {
                    bufferPool.free(buffer);
                    buffer = bufferPool.allocate(recordSize);

                    buffer.limit(recordSize - RecordHeader.SECONDARY_HEADER); //limit to the entry size, excluding the secondary header
                    long readStart = position - recordSize;
                    storage.read(readStart, buffer);
                    buffer.flip();

                    int foundLength = buffer.getInt();
                    checkRecordLength(foundLength, position);
                    int checksum = buffer.getInt();
                    checksum(checksum, buffer, position);
                    return BufferRef.of(buffer, bufferPool);
                }

                buffer.limit(recordDataEnd);
                buffer.position(recordDataEnd - length - RecordHeader.CHECKSUM_SIZE);
                int checksum = buffer.getInt();
                checksum(checksum, buffer, position);
                return BufferRef.of(buffer, bufferPool);

            } finally {
                bufferPool.free(buffer);
            }

        }
    }

    private void checkRecordLength(int length, long position) {
        if (length < 0 || length > maxEntrySize) {
            throw new IllegalStateException("Invalid record length " + length + " at position " + position);
        }
    }

}
