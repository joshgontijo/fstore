package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import org.xnio.BufferAllocator;
import org.xnio.ByteBufferSlicePool;
import org.xnio.Pool;

import java.nio.ByteBuffer;

public class BufferTest {

    public static void main(String[] args) {

        int bufferSize = Size.KB.ofInt(8);
        Pool<ByteBuffer> pool = new ByteBufferSlicePool(BufferAllocator.BYTE_BUFFER_ALLOCATOR, bufferSize, bufferSize * 128);

        Threads.sleep(5000);
        BufferTest bufferTest = new BufferTest();
        ByteBuffer[] buffers = new ByteBuffer[2];
        for (int i = 0; i < 2; i++) {
            ByteBuffer b = Buffers.allocate(1024 * 10, false);
            buffers[i] = b.limit(b.position());

        }
        int i = 0;
        while (true) {
            int garbage = bufferTest.createGarbage(buffers);
            if (garbage == 1234) {
                System.out.println();
            }
//            if(i++ % 1000 == 0) {
//                Threads.sleep(1);
//            }
        }

    }

    public int createGarbage(ByteBuffer[] buffers) {
        ByteBuffer ref = Buffers.allocate(10, false);
        return ref.remaining();
    }

}
