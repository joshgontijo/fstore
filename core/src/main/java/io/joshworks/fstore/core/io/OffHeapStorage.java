package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class OffHeapStorage extends MemStorage {

    OffHeapStorage(String name, long size) {
        super(name, size, OffHeapStorage::create);
    }

    private static ByteBuffer create(long from, int bufferSize) {
        return ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    protected void destroy(ByteBuffer buffer) {
        buffer.clear(); //just reset the buffer, no data will be deleted
    }
}