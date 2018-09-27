package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;
import java.util.Random;

//THREAD SAFE
public class FixedBufferDataReader implements DataStream {

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    protected static final double DEFAULT_CHECKUM_PROB = 1;

    protected final int maxRecordSize;
    private final double checksumProb;
    private final Random rand = new Random();
    private final boolean direct;
    private final int bufferSize;

    public FixedBufferDataReader(int maxRecordSize) {
        this(maxRecordSize, false);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct) {
        this(maxRecordSize, direct, DEFAULT_CHECKUM_PROB, DEFAULT_BUFFER_SIZE);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct, double checksumProb) {
        this(maxRecordSize, direct, checksumProb, DEFAULT_BUFFER_SIZE);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct, double checksumProb, int bufferSize) {
        this.maxRecordSize = maxRecordSize;
        this.checksumProb = (int) (checksumProb * 100);
        this.direct = direct;
        this.bufferSize = bufferSize;
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }
    }


    @Override
    public int write(Storage storage, BufferPool bufferPool, ByteBuffer bytes) {
        int recordSize = Log.HEADER_OVERHEAD + bytes.remaining();

        ByteBuffer bb = bufferPool.allocate(recordSize);
        try {
            int entrySize = bytes.remaining();
            bb.putInt(entrySize);
            bb.putInt(Checksum.crc32(bytes));
            bb.put(bytes);
            bb.putInt(entrySize);

            bb.flip();
            return storage.write(bb);

        } finally {
            bufferPool.free(bb);
        }
    }

    @Override
    public DataReader reader(Storage storage, BufferPool bufferPool, Direction direction) {
        return Direction.FORWARD.equals(direction) ? new ForwardDataReader(storage, bufferPool) : new BackwardDataReader(storage, bufferPool);
    }


//    private void checkRecordLength(int length, long position) {
//        if (length > maxRecordSize) {
//            throw new IllegalStateException("Record at position " + position + " of size " + length + " must be less than MAX_RECORD_SIZE: " + maxRecordSize);
//        }
//    }

//    private ByteBuffer extending(Storage storage, long position, int length) {
//        ByteBuffer extra = ByteBuffer.allocate(Log.MAIN_HEADER + length);
//        storage.read(position, extra);
//        extra.flip();
//        int foundLength = extra.getInt();
//        if (foundLength != length) {
//            throw new IllegalStateException("Record at position " + position + " has unexpected length, expected " + length + ", got " + foundLength);
//        }
//        int checksum = extra.getInt();
//        checksum(checksum, extra, position);
//        return extra;
//    }

    private void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (rand.nextInt(100) < checksumProb && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }

    private final class ForwardDataReader implements DataReader {

        private final BufferPool bufferPool;
        private final Storage storage;

        private ForwardDataReader(Storage storage, BufferPool bufferPool) {
            this.storage = storage;
            this.bufferPool = bufferPool;
        }

        @Override
        public ByteBufferReference read(long position) {
            //TODO define correct size
            ByteBuffer buffer = bufferPool.allocate(1024);
            storage.read(position, buffer);
            buffer.flip();

            if (buffer.remaining() == 0) {
                return ByteBufferReference.of(EMPTY);
            }

            int length = buffer.getInt();
            if (length == 0) {
                return ByteBufferReference.of(EMPTY);
            }

            int recordSize = length + Log.MAIN_HEADER;
            if (recordSize > buffer.capacity()) {
                bufferPool.free(buffer);
                buffer = bufferPool.allocate(recordSize);
                storage.read(position, buffer);
                buffer.flip();
                buffer.getInt(); //skip length
            }

            int checksum = buffer.getInt();
            buffer.limit(buffer.position() + length);
            checksum(checksum, buffer, position);
            return ByteBufferReference.of(buffer);

        }
    }

    private final class BackwardDataReader implements DataReader {

        private final BufferPool bufferPool;
        private final Storage storage;

        private BackwardDataReader(Storage storage, BufferPool bufferPool) {
            this.storage = storage;
            this.bufferPool = bufferPool;
        }

        @Override
        public ByteBufferReference read(long position) {
            //TODO initial size must be defined
            ByteBuffer buffer = bufferPool.allocate(4096);
            int limit = buffer.limit();
            if (position - limit < Log.START) {
                int available = (int) (position - Log.START);
                if (available == 0) {
                    return ByteBufferReference.of(EMPTY);
                }
                buffer.limit(available);
                limit = available;
            }

            storage.read(position - limit, buffer);
            buffer.flip();
            int originalSize = buffer.remaining();
            if (buffer.remaining() == 0) {
                return ByteBufferReference.of(EMPTY);
            }

            buffer.position(buffer.limit() - Log.LENGTH_SIZE);
            buffer.mark();
            int length = buffer.getInt();
//        checkRecordLength(length, position);
            if (length == 0) {
                return ByteBufferReference.of(EMPTY);
            }

            if (length + Log.HEADER_OVERHEAD > buffer.capacity()) {
                return extending(storage, position, length);
            }

            buffer.reset();
            buffer.limit(buffer.position());
            buffer.position(buffer.position() - length - Log.CHECKSUM_SIZE);
            int checksum = buffer.getInt();
            checksum(checksum, buffer, position);
            return ByteBufferReference.of(buffer);
        }
    }


}
