package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.DataReader;
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

    private static final int DEFAULT_NUM_BUFFERS = 1000;
    private static final double DEFAULT_CHECKSUM_PROB = 1;

    byte[] EOL = ByteBuffer.allocate(FixedBufferDataStream.MAIN_HEADER).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc
    long START = Header.BYTES;

    private static final int RECORD_START_POS = MAIN_HEADER;

    protected final int maxRecordSize;
    protected final int checksumProb;
    private final Random rand = new Random();

    public FixedBufferDataStream(int maxRecordSize, Serializer<T> serializer) {
        this(maxRecordSize, DEFAULT_NUM_BUFFERS, DEFAULT_CHECKSUM_PROB, false, serializer);
    }

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

            int endOfDataPos = buffer.position();
            buffer.position(RECORD_START_POS);
            buffer.limit(endOfDataPos);
            int length = endOfDataPos - RECORD_START_POS;
            int checksum = Checksum.crc32(buffer);

            //write main header
            buffer.position(0);
            buffer.putInt(length);
            buffer.putInt(checksum);

            //write secondary header
            buffer.limit(endOfDataPos + SECONDARY_HEADER);
            buffer.position(endOfDataPos);
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
    public DataReader<T> reader(Storage storage, long position) {
        return new FixedBufferDataReader(storage, position);
    }


    private void checkRecordLength(int length, long position) {
        if (length > maxRecordSize) {
            throw new IllegalStateException("Record at position " + position + " of size " + length + " must be less than MAX_RECORD_SIZE: " + maxRecordSize);
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


    private class FixedBufferDataReader implements DataReader<T> {

        private final Storage storage;
        private long position;

        public FixedBufferDataReader( Storage storage, long position) {
            this.storage = storage;
            this.position = position;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public DataReader<T> position(long position) {
            this.position = position;
            return this;
        }

        @Override
        public T readForward() {
            if(head()) {
                return null;
            }
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

                position += length + HEADER_OVERHEAD;

                return serializer.fromBytes(buffer);
            } finally {
                bufferPool.free(buffer);
            }
        }

        @Override
        public T readBackward() {
            if(head()) {
                return null;
            }
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

                position -= length + HEADER_OVERHEAD;

                //TODO difrect serializer may leak buffer
                return serializer.fromBytes(buffer);

            } finally {
                bufferPool.free(buffer);
            }
        }

        @Override
        public boolean head() {
            return position >= storage.position();
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


}
