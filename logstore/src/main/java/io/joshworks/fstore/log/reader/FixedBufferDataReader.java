package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.DataStream;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;
import io.joshworks.fstore.log.segment.Header;

import java.nio.ByteBuffer;
import java.util.Random;

//THREAD SAFE
public class FixedBufferDataReader<T> extends DataStream<T> {


    private static final int LENGTH_SIZE = Integer.BYTES; //length
    private static final int CHECKSUM_SIZE = Integer.BYTES; //crc32
    private static final int MAIN_HEADER = LENGTH_SIZE + CHECKSUM_SIZE; //header before the entry
    private static final int SECUNDARY_HEADER = LENGTH_SIZE; //header after the entry, used only for backward reads

    int HEADER_OVERHEAD = MAIN_HEADER + LENGTH_SIZE; //length + crc32
    byte[] EOL = ByteBuffer.allocate(HEADER_OVERHEAD).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc
    long START = Header.BYTES;

    private static final int RECORD_START_POS = MAIN_HEADER;

    protected static final double DEFAULT_CHECKUM_PROB = 1;

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    public static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    protected final int maxRecordSize;
//    private final boolean direct;
    private final int bufferSize;


    protected final int checksumProb;
    private final Random rand = new Random();


    public FixedBufferDataReader(int maxRecordSize) {
        this(maxRecordSize, false);
    }

    public FixedBufferDataReader(int maxRecordSize, boolean direct) {
        this(maxRecordSize, direct, DEFAULT_CHECKUM_PROB, DEFAULT_BUFFER_SIZE);
    }

    public FixedBufferDataReader(int maxRecordSize, double checksumProb, BufferPool bufferPool, Storage storage, Serializer<T> serializer) {
        super(bufferPool, storage, serializer);
        this.maxRecordSize = maxRecordSize;
        this.checksumProb = (int) (checksumProb * 100);
//        this.direct = direct;
//        this.bufferSize = bufferSize;
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }

    }


    @Override
    public long write(T data) {
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
                return EMPTY;
            }

            int length = buffer.getInt();
            checkRecordLength(length, position);
            if (length == 0) {
                return EMPTY;
            }
            if (length + MAIN_HEADER > buffer.capacity()) {
                return extending(storage, position, length);
            }

            int checksum = buffer.getInt();
            buffer.limit(buffer.position() + length);
            checksum(checksum, buffer, position);
            return buffer;
        } finally {
            bufferPool.free(buffer);
        }

    }

    @Override
    public T readBackward(Storage storage, long position) {

        ByteBuffer buffer = getBuffer();
        int limit = buffer.limit();
        if (position - limit < START) {
            int available = (int) (position - START);
            if (available == 0) {
                return EMPTY;
            }
            buffer.limit(available);
            limit = available;
        }

        storage.read(position - limit, buffer);
        buffer.flip();
        int originalSize = buffer.remaining();
        if (buffer.remaining() == 0) {
            return EMPTY;
        }

        buffer.position(buffer.limit() - LENGTH_SIZE);
        buffer.mark();
        int length = buffer.getInt();
        checkRecordLength(length, position);
        if (length == 0) {
            return EMPTY;
        }

        if (length + HEADER_OVERHEAD > buffer.capacity()) {
            return extendingBackwards(storage, position, length);
        }

        buffer.reset();
        buffer.limit(buffer.position());
        buffer.position(buffer.position() - length - CHECKSUM_SIZE);
        int checksum = buffer.getInt();
        checksum(checksum, buffer, position);
        return buffer;
    }

    private void checkRecordLength(int length, long position) {
        if (length > maxRecordSize) {
            throw new IllegalStateException("Record at position " + position + " of size " + length + " must be less than MAX_RECORD_SIZE: " + maxRecordSize);
        }
    }

    private ByteBuffer extending(Storage storage, long position, int length) {
        ByteBuffer extra = ByteBuffer.allocate(MAIN_HEADER + length);
        storage.read(position, extra);
        extra.flip();
        int foundLength = extra.getInt();
        if (foundLength != length) {
            throw new IllegalStateException("Record at position " + position + " has unexpected length, expected " + length + ", got " + foundLength);
        }
        int checksum = extra.getInt();
        checksum(checksum, extra, position);
        return extra;
    }

    private ByteBuffer extendingBackwards(Storage storage, long position, int length) {
        ByteBuffer extra = ByteBuffer.allocate(HEADER_OVERHEAD + length);
        storage.read(position - extra.limit(), extra);
        extra.flip();
        extra.limit(extra.limit() - SECUNDARY_HEADER);
        int foundLength = extra.getInt();
        if (foundLength != length) {
            throw new IllegalStateException("Record at position " + position + " has unexpected length, expected " + length + ", got " + foundLength);
        }
        int checksum = extra.getInt();
        checksum(checksum, extra, position);
        return extra;
    }

    private void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (rand.nextInt(100) < checksumProb && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }

}
