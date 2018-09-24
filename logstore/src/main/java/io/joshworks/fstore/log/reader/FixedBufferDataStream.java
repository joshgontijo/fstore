package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.DataStream;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;
import io.joshworks.fstore.log.segment.Header;

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.Random;

//THREAD SAFE
public class FixedBufferDataStream<T> extends DataStream<T> {



    byte[] EOL = ByteBuffer.allocate(FixedBufferDataStream.MAIN_HEADER).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc
    long START = Header.BYTES;

    private static final int RECORD_START_POS = MAIN_HEADER;

    protected final int maxRecordSize;
    protected final int checksumProb;
    private final Random rand = new Random();

    public FixedBufferDataStream(int maxRecordSize, int numBuffers, double checksumProb, boolean direct, Serializer<T> serializer) {
        super(new BufferPool(maxRecordSize, numBuffers, direct), serializer);
        this.maxRecordSize = maxRecordSize;
        this.checksumProb = (int) (checksumProb * 100);
    }

    @Override
    public long write(Storage storage, T data) {
        ByteBuffer buffer = bufferPool.allocate();
        try {
            long position = storage.position();
            buffer.position(RECORD_START_POS);

            serializer.writeTo(data, buffer);

            buffer.mark();
            buffer.position(RECORD_START_POS);
            int length = buffer.remaining();
            int checksum = Checksum.crc32(buffer);

            buffer.position(0);
            buffer.putInt(length);
            buffer.putInt(checksum);

            buffer.reset();
            buffer.putInt(length);

            buffer.flip();

            storage.write(buffer);
            return position;

        } catch (BufferOverflowException e) {
            throw new RuntimeException("Record is bigger than the buffer size of " + buffer.capacity());
        } finally {
            bufferPool.free(buffer);
        }
    }

    @Override
    public T readForward(Storage storage, long position) {
        ByteBuffer buffer = bufferPool.allocate();
        try {
            storage.read(position, buffer);
            buffer.flip();

            if (buffer.remaining() == 0) {
                return null;
            }

            int length = buffer.getInt();
            checkRecordLength(length, position);
            if (length == 0) {
                return null;
            }
            if (length + MAIN_HEADER > buffer.capacity()) {
//                return extending(storage, position, length);
                throw new RuntimeException("Record of size of (" + length + ") is bigger than the " + maxRecordSize);
            }

            int checksum = buffer.getInt();
            buffer.limit(buffer.position() + length);
            checksum(checksum, buffer, position);
            return serializer.fromBytes(buffer);
        } finally {
            bufferPool.free(buffer);
        }
    }

    @Override
    public T readBackward(Storage storage, long position) {
        ByteBuffer buffer = bufferPool.allocate();
        try {
            int limit = buffer.limit();
            if (position - limit < START) {
                int available = (int) (position - START);
                if (available == 0) {
                    return null;
                }
                buffer.limit(available);
                limit = available;
            }

            storage.read(position - limit, buffer);
            buffer.flip();
            int originalSize = buffer.remaining();
            if (buffer.remaining() == 0) {
                return null;
            }

            buffer.position(buffer.limit() - LENGTH_SIZE);
            buffer.mark();
            int length = buffer.getInt();
            checkRecordLength(length, position);
            if (length == 0) {
                return null;
            }

            if (length + HEADER_OVERHEAD > buffer.capacity()) {
//                return extendingBackwards(storage, position, length);
                throw new RuntimeException("Record of size of (" + length + ") is bigger than the " + maxRecordSize);
            }

            buffer.reset();
            buffer.limit(buffer.position());
            buffer.position(buffer.position() - length - CHECKSUM_SIZE);
            int checksum = buffer.getInt();
            checksum(checksum, buffer, position);
            //TODO difrect serializer may leak buffer
            return serializer.fromBytes(buffer);

        } finally {
            bufferPool.free(buffer);
        }
    }

    private void checkRecordLength(int length, long position) {
        if (length > maxRecordSize) {
            throw new IllegalStateException("Record at position " + position + " of size " + length + " must be less than MAX_RECORD_SIZE: " + maxRecordSize);
        }
    }

//    private T extending(Storage storage, long position, int length) {
//        ByteBuffer extra = ByteBuffer.allocate(MAIN_HEADER + length);
//        storage.read(position, extra);
//        extra.flip();
//        int foundLength = extra.getInt();
//        if (foundLength != length) {
//            throw new IllegalStateException("Record at position " + position + " has unexpected length, expected " + length + ", got " + foundLength);
//        }
//        int checksum = extra.getInt();
//        checksum(checksum, extra, position);
//        return serializer.fromBytes(extra);
//    }
//
//    private T extendingBackwards(Storage storage, long position, int length) {
//        ByteBuffer extra = ByteBuffer.allocate(HEADER_OVERHEAD + length);
//        storage.read(position - extra.limit(), extra);
//        extra.flip();
//        extra.limit(extra.limit() - SECONDARY_HEADER);
//        int foundLength = extra.getInt();
//        if (foundLength != length) {
//            throw new IllegalStateException("Record at position " + position + " has unexpected length, expected " + length + ", got " + foundLength);
//        }
//        int checksum = extra.getInt();
//        checksum(checksum, extra, position);
//        return serializer.fromBytes(extra);;
//    }

    private void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (rand.nextInt(100) < checksumProb && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }

}
