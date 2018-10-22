package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Random;

//THREAD SAFE
public class DataStream implements IDataStream {

    private static final double DEFAULT_CHECKUM_PROB = 1;
    private static final int MAX_CACHE_RESULT = 100;

    private static final int READ_BUFFER_SIZE = Memory.PAGE_SIZE;
    private static final int BULK_READ_BUFFER_SIZE = Memory.PAGE_SIZE * 2;

    //hard limit is required to memory issues in case of broken record
    public static final int MAX_ENTRY_SIZE = 1024 * 1024 * 5;

    private final double checksumProb;
    private final Random rand = new Random();
    private final RecordReader forwardReader = new ForwardRecordReader();
    private final RecordReader bulkForwardReader = new BulkForwardRecordReader();
    private final RecordReader bulkBackwardReader = new BulkBackwardRecordReader();
    private final RecordReader backwardReader = new BackwardRecordReader();
    private final BufferPool bufferPool;

    public DataStream(BufferPool bufferPool) {
        this(bufferPool, DEFAULT_CHECKUM_PROB);
    }

    public DataStream(BufferPool bufferPool, double checksumProb) {
        this.checksumProb = (int) (checksumProb * 100);
        this.bufferPool = Objects.requireNonNull(bufferPool, "BufferPool must be provided");
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }
    }

    @Override
    public long write(Storage storage, ByteBuffer bytes) {
        int recordSize = RecordHeader.HEADER_OVERHEAD + bytes.remaining();
        long storagePos = storage.position();
        if (storagePos < Log.START) {
            throw new IllegalStateException("storage position is less than " + Log.START);
        }

        if (recordSize > MAX_ENTRY_SIZE) {
            throw new IllegalArgumentException("Record cannot exceed " + MAX_ENTRY_SIZE + " bytes");
        }

        ByteBuffer bb = bufferPool.allocate(recordSize);
        try {
            int entrySize = bytes.remaining();
            bb.putInt(entrySize);
            bb.putInt(Checksum.crc32(bytes));
            bb.put(bytes);
            bb.putInt(entrySize);

            bb.flip();
            storage.write(bb);
            return storagePos;

        } finally {
            bufferPool.free(bb);
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
            throw new IllegalStateException("Failed to bulk read at position " + position, e);
        }
    }

    private void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
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

                if (buffer.remaining() == 0) {
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
            } catch (Exception e) {
                bufferPool.free(buffer);
                throw new RuntimeException(e);
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

                if (buffer.remaining() == 0) {
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
                int[] markers = new int[MAX_CACHE_RESULT];
                int[] lengths = new int[MAX_CACHE_RESULT];
                int i = 0;
                while (buffer.hasRemaining() && buffer.remaining() > RecordHeader.MAIN_HEADER && i < MAX_CACHE_RESULT) {
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
                    buffer.limit(buffer.capacity());

                    i++;
                    int newPos = buffer.position() + len + RecordHeader.SECONDARY_HEADER;
                    if (newPos > buffer.limit()) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    buffer.position(newPos);
                }
                return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
            } catch (Exception e) {
                bufferPool.free(buffer);
                throw new RuntimeException(e);
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

                int[] markers = new int[MAX_CACHE_RESULT];
                int[] lengths = new int[MAX_CACHE_RESULT];
                int i = 0;
                int lastRecordPos = buffer.limit();
                while (i < MAX_CACHE_RESULT) {

                    buffer.limit(buffer.capacity());
                    buffer.position(lastRecordPos);

                    int len = buffer.getInt(lastRecordPos - RecordHeader.SECONDARY_HEADER);
                    int recordStart = lastRecordPos - len - RecordHeader.HEADER_OVERHEAD;
                    if (recordStart < 0) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                    buffer.position(lastRecordPos - len - RecordHeader.HEADER_OVERHEAD);
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

                    lastRecordPos = pos;
                    i++;

                    if (lastRecordPos - RecordHeader.SECONDARY_HEADER <= 0) {
                        return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
                    }
                }

                return BufferRef.withMarker(buffer, bufferPool, markers, lengths, i);
            } catch (Exception e) {
                bufferPool.free(buffer);
                throw new RuntimeException(e);
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

            } catch (Exception e) {
                bufferPool.free(buffer);
                throw new RuntimeException(e);
            }

        }
    }

    private void checkRecordLength(int length, long position) {
        if (length < 0 || length > MAX_ENTRY_SIZE) {
            throw new IllegalStateException("Invalid length " + length + " at position " + position);
        }
    }

}
