package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class OffHeapStorage extends MemoryStorage {

    public OffHeapStorage(long length) {
        super(length);
    }

    @Override
    protected ByteBuffer create(int bufferSize) {
        return ByteBuffer.allocateDirect(bufferSize);
    }
}