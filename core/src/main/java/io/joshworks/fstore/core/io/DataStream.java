package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.Serializer;

public abstract class DataStream<T> {

    protected final BufferPool bufferPool;
    protected final Storage storage;
    protected final Serializer<T> serializer;

    protected DataStream(BufferPool bufferPool, Storage storage, Serializer<T> serializer) {
        this.bufferPool = bufferPool;
        this.storage = storage;
        this.serializer = serializer;
    }

    public abstract long write(T data);

    public abstract T readForward(Storage storage, long position);

    public abstract T readBackward(Storage storage, long position);

}
