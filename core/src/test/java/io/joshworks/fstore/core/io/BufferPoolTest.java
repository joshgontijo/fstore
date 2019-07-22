package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class BufferPoolTest {

    private BufferPool bufferPool;

    @Before
    public void setUp() {
        bufferPool = new BufferPool(Memory.PAGE_SIZE, false);
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
        bufferPool.free();

        allocated = bufferPool.allocate(size);
        assertEquals(size, allocated.limit());
    }

    @Test(expected = IllegalArgumentException.class)
    public void trying_to_allocate_buffer_greater_than_max_size_throws_exception() {
        bufferPool.allocate(bufferPool.capacity() + 1);
    }

    //SingleBufferThreadCachedPool specific
    @Test(expected = IllegalStateException.class)
    public void only_single_buffer_can_be_allocated_per_thread() {
        bufferPool.allocate(10);
        bufferPool.allocate(10);

    }

}