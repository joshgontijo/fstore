package io.joshworks.fstore.log.reader;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.RecordReader;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Checksum;
import io.joshworks.fstore.log.ChecksumException;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;

import java.nio.ByteBuffer;
import java.util.Random;

//THREAD SAFE
public class DataStream<T> {

    private static final int DEFAULT_NUM_BUFFERS = 1000;
    private static final double DEFAULT_CHECKSUM_PROB = 1;

    public static final int LENGTH_SIZE = Integer.BYTES; //length
    public static final int CHECKSUM_SIZE = Integer.BYTES; //crc32
    public static final int MAIN_HEADER = LENGTH_SIZE + CHECKSUM_SIZE; //header before the entry
    public static final int SECONDARY_HEADER = LENGTH_SIZE; //header after the entry, used only for backward reads

    public static final int HEADER_OVERHEAD = MAIN_HEADER + SECONDARY_HEADER; //length + crc32

    public static final byte[] EOL = ByteBuffer.allocate(MAIN_HEADER).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc

//    public static final long START = Header.BYTES;

    private static final int RECORD_START_POS = MAIN_HEADER;

    private final int checksumProb;
    private final Random rand = new Random();

    private final BufferPool bufferPool;
    private final Serializer<T> serializer;


    public DataStream(Serializer<T> serializer, BufferPool bufferPool) {
        this(DEFAULT_CHECKSUM_PROB,  serializer, bufferPool);
    }

    public DataStream(double checksumProb, Serializer<T> serializer, BufferPool bufferPool) {
        if(checksumProb < 0) throw new IllegalArgumentException("checksumProb must be at least zero");
        this.bufferPool = bufferPool;
        this.serializer = serializer;
        this.checksumProb = (int) (checksumProb * 100);
    }

    public long write(Storage storage, T data) {
        ByteBuffer dataBuffer = serializer.toBytes(data);
        int recordSize = dataBuffer.remaining() + DataStream.HEADER_OVERHEAD;

        ByteBuffer buffer = bufferPool.allocate(recordSize);
        try {
            long position = storage.position();

            int length = dataBuffer.remaining();
            int checksum = Checksum.crc32(dataBuffer);

            //write main header
            buffer.putInt(length);
            buffer.putInt(checksum);

            buffer.put(dataBuffer);

            //write secondary header
            buffer.putInt(length);

            buffer.flip();

            storage.write(buffer);
            return position;

        } finally {
            bufferPool.free(buffer);
        }
    }

    public RecordReader<T> reader(Storage storage, long position, Direction direction) {
        if(position < Log.START) {
            throw new IllegalArgumentException("Position must be greater than Log.START " + position);
        }
        if(Direction.FORWARD.equals(direction)) {

        }
        return Direction.FORWARD.equals(direction) ? new ForwardRecordReader(storage, position) : new BackwardRecordReader(storage, position);
    }

//    private void checkRecordLength(int length, long position) {
//        if (length > maxRecordSize) {
//            throw new IllegalStateException("Record at position " + position + " of size " + length + " must be less than MAX_RECORD_SIZE: " + maxRecordSize);
//        }
//    }

    private void checksum(int expected, ByteBuffer data, long position) {
        if (checksumProb == 0) {
            return;
        }
        if (rand.nextInt(100) < checksumProb && Checksum.crc32(data) != expected) {
            throw new ChecksumException(position);
        }
    }


    private class ForwardRecordReader implements RecordReader<T> {

        private final Storage storage;
        private long position;

        public ForwardRecordReader(Storage storage, long position) {
            this.storage = storage;
            this.position = position;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public RecordReader<T> position(long position) {
            this.position = position;
            return this;
        }

        @Override
        public T readNext() {
            ByteBuffer buffer = bufferPool.allocate(Memory.PAGE_SIZE);
            try {
                storage.read(position, buffer);
                buffer.flip();

                if (buffer.remaining() == 0) {
                    return null;
                }

                int length = buffer.getInt();
                if (length == 0) {
                    return null;
                }

                if(length + MAIN_HEADER > buffer.capacity()) {
                    bufferPool.free(buffer);
                    buffer = bufferPool.allocate(length + MAIN_HEADER);

                    storage.read(position, buffer);
                    buffer.flip();
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

    }

    private class BackwardRecordReader implements RecordReader<T> {

        private final Storage storage;
        private long position;

        public BackwardRecordReader(Storage storage, long position) {
            this.storage = storage;
            this.position = position;
        }

        @Override
        public long position() {
            return position;
        }

        @Override
        public RecordReader<T> position(long position) {
            this.position = position;
            return this;
        }

        @Override
        public T readNext() {
            ByteBuffer buffer = bufferPool.allocate(Memory.PAGE_SIZE); //how to get initial size
            try {
                int bufferSize = buffer.limit();
                if (position - bufferSize < Log.START) {
                    int available = (int) (position - Log.START);
                    if (available == 0) {
                        return null;
                    }
                    buffer.limit(available);
                    bufferSize = available;
                }

                storage.read(position - bufferSize, buffer);
                buffer.flip();
//                int originalSize = buffer.remaining();
                if (buffer.remaining() == 0) {
                    return null;
                }

                int dataEnd = buffer.limit() - LENGTH_SIZE;
                buffer.position(dataEnd);
                int length = buffer.getInt();
                if (length == 0) {
                    return null;
                }

                if(length + HEADER_OVERHEAD > buffer.capacity()) {
                    bufferPool.free(buffer);
                    int recordLength = length + HEADER_OVERHEAD;

                    buffer = bufferPool.allocate(recordLength);
                    buffer.limit(recordLength);

                    storage.read(position - recordLength, buffer);
                    buffer.flip();
                }

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
