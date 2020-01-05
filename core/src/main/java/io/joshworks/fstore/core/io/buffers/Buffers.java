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

    public static int absolutePut(ByteBuffer src, int srcPos, ByteBuffer dst) {
        if (srcPos > src.position()) {
            return 0;
        }
        //do not use src.remaining() here as we don't expect flipped buffers
        int srcRemaining = src.position() - srcPos;
        int readableBytes = Math.min(srcRemaining, dst.remaining());
        int srcEndPos = srcPos + readableBytes;
        for (int i = srcPos; i < srcEndPos; i++) {
            dst.put(src.get(i));
        }
        return readableBytes;
    }

    public static void offsetPosition(ByteBuffer data, int offset) {
        data.position(data.position() + offset);
    }
}
