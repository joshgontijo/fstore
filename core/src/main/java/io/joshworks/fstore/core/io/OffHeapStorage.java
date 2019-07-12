package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

/**
 * Heap storage, closing have no effect on the data, it will remain available until {@link Storage#delete()} is called
 */
public class OffHeapStorage extends MemStorage {

    OffHeapStorage(String name, long size) {
        super(name, size, OffHeapStorage::create);
    }

    private static ByteBuffer create(long from, int bufferSize) {
        return ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    protected void destroy(ByteBuffer buffer) {
    }

    @Override
    public void delete() {
        buffers.clear();
        super.delete();
    }
}