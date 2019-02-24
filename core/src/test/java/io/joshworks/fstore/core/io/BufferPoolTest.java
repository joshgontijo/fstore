package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class BufferPoolTest {

    private BufferPool bufferPool;

    @Before
    public void setUp() {
        bufferPool = new SingleBufferThreadCachedPool(false);
    }

    @Test
    public void allocate_matches_the_requested_size() {
        int size = 1024;
        ByteBuffer allocated = bufferPool.allocate(size);
        assertEquals(size, allocated.limit());
    }

    @Test
    public void allocate_always_provides_cleared_buffers() {
        int size = 1024;
        ByteBuffer allocated = bufferPool.allocate(size);
        allocated.putLong(1);
        bufferPool.free(allocated);

        allocated = bufferPool.allocate(size);
        assertEquals(size, allocated.limit());
    }

    //SingleBufferThreadCachedPool specific
    @Test(expected = IllegalStateException.class)
    public void only_single_buffer_can_be_allocated_per_thread() {
        bufferPool.allocate(10);
        bufferPool.allocate(10);

    }

}