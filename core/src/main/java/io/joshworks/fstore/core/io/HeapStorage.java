package io.joshworks.fstore.core.io;

import java.nio.ByteBuffer;

/**
 * Heap storage, closing have no effect on the data, it will remain available until {@link Storage#delete()} is called
 */
public class HeapStorage extends MemStorage {

    HeapStorage(String name, long size) {
        super(name, size, HeapStorage::create);
    }

    private static ByteBuffer create(long from, int bufferSize) {
        return ByteBuffer.allocate(bufferSize);
    }

    @Override
    protected void destroy(ByteBuffer buffer) {
        //do nothing
    }

    @Override
    public void delete() {
        buffers.clear();
        super.delete();
    }
}