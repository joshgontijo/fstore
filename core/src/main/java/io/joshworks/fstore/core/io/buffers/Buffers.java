package io.joshworks.fstore.core.io.buffers;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.MemStorage.MAX_BUFFER_SIZE;

public class Buffers {

    public static ByteBuffer allocate(int size, boolean direct) {
        if (size >= MAX_BUFFER_SIZE) {
            throw new IllegalArgumentException("Buffer too large: Max allowed size is: " + MAX_BUFFER_SIZE);
        }
        return direct ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
    }


}
