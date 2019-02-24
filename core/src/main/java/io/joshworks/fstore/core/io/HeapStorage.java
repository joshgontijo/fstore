package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

public class HeapStorage extends MemStorage {

    public HeapStorage(String name, long size) {
        super(name, size, HeapStorage::create);
    }

    public HeapStorage(String name, long size, int bufferSize) {
        super(name, size, bufferSize, HeapStorage::create);
    }

    private static ByteBuffer create(long from, int bufferSize) {
        return ByteBuffer.allocate(bufferSize);
    }

    @Override
    protected void destroy(ByteBuffer buffer) {
        buffer.clear(); //just reset the buffer, no data will be deleted
    }
}