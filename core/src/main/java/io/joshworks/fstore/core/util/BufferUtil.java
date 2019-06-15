package io.joshworks.fstore.core.util;

import java.nio.ByteBuffer;
import java.util.List;

public class BufferUtil {

    public static ByteBuffer getBuffer(List<ByteBuffer> buffers, long pos) {
        int idx = bufferIdx(buffers, pos);
        return buffers.get(idx);
    }

    public static int posOnBuffer(List<ByteBuffer> buffers, long pos) {
        long total = 0;
        for (ByteBuffer buffer : buffers) {
            int capacity = buffer.capacity();
            if (total + capacity > pos) {
                return (int) (pos - total);
            }
            total += capacity;
        }
        return -1;
    }

    public static int bufferIdx(List<ByteBuffer> buffers, long pos) {
        long total = 0;
        int idx = 0;
        for (ByteBuffer buffer : buffers) {
            total += buffer.capacity();
            if (total > pos) {
                return idx;
            }
            idx++;
        }
        return pos > total ? -1 : idx;
    }

}
