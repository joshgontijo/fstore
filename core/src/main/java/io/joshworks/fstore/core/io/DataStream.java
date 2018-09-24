package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public abstract class DataStream<T> {

    public static final int LENGTH_SIZE = Integer.BYTES; //length
    public static final int CHECKSUM_SIZE = Integer.BYTES; //crc32
    public static final int MAIN_HEADER = LENGTH_SIZE + CHECKSUM_SIZE; //header before the entry
    public static final int SECONDARY_HEADER = LENGTH_SIZE; //header after the entry, used only for backward reads

    public static final int HEADER_OVERHEAD = MAIN_HEADER + SECONDARY_HEADER; //length + crc32

    public static final byte[] EOL = ByteBuffer.allocate(MAIN_HEADER).putInt(0).putInt(0).array(); //eof header, -1 length, 0 crc


    protected final BufferPool bufferPool;
    protected final Serializer<T> serializer;

    protected DataStream(BufferPool bufferPool,  Serializer<T> serializer) {
        this.bufferPool = bufferPool;
        this.serializer = serializer;
    }

    public abstract long write(Storage storage, T data);

    public abstract T readForward(Storage storage, long position);

    public abstract T readBackward(Storage storage, long position);

}
