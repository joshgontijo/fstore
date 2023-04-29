package io.joshworks.fstore.core.util;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class BufferUtilTest {

    @Test
    public void bufferIdx() {
        var buffer1 = ByteBuffer.allocate(10);
        var buffer2 = ByteBuffer.allocate(10);
        var buffer3 = ByteBuffer.allocate(10);
        var buffers = List.of(buffer1, buffer2, buffer3);

        assertEquals(0, BufferUtil.bufferIdx(buffers, 0));
        assertEquals(0, BufferUtil.bufferIdx(buffers, 5));
        assertEquals(1, BufferUtil.bufferIdx(buffers, 10));
        assertEquals(1, BufferUtil.bufferIdx(buffers, 15));
        assertEquals(2, BufferUtil.bufferIdx(buffers, 20));
        assertEquals(2, BufferUtil.bufferIdx(buffers, 25));
    }

    @Test
    public void bufferIdx_returns_minus_one_if_pos_is_greater_than_total_buffer_size() {
        var buffer1 = ByteBuffer.allocate(10);
        var buffer2 = ByteBuffer.allocate(10);
        var buffer3 = ByteBuffer.allocate(10);
        var buffers = List.of(buffer1, buffer2, buffer3);

        assertEquals(-1, BufferUtil.bufferIdx(buffers, 31));
    }

    @Test
    public void bufferIdx_uneven_sizes() {
        var buffer1 = ByteBuffer.allocate(10);
        var buffer2 = ByteBuffer.allocate(2);
        var buffer3 = ByteBuffer.allocate(50);
        var buffers = List.of(buffer1, buffer2, buffer3);

        assertEquals(0, BufferUtil.bufferIdx(buffers, 0));
        assertEquals(0, BufferUtil.bufferIdx(buffers, 5));
        assertEquals(1, BufferUtil.bufferIdx(buffers, 10));
        assertEquals(2, BufferUtil.bufferIdx(buffers, 15));
        assertEquals(2, BufferUtil.bufferIdx(buffers, 20));
        assertEquals(2, BufferUtil.bufferIdx(buffers, 25));
    }

    @Test
    public void posOnBuffer() {
        var buffer1 = ByteBuffer.allocate(10);
        var buffer2 = ByteBuffer.allocate(10);
        var buffer3 = ByteBuffer.allocate(10);
        var buffers = List.of(buffer1, buffer2, buffer3);

        assertEquals(0, BufferUtil.posOnBuffer(buffers, 0));
        assertEquals(9, BufferUtil.posOnBuffer(buffers, 9));
        assertEquals(0, BufferUtil.posOnBuffer(buffers, 10));
        assertEquals(9, BufferUtil.posOnBuffer(buffers, 19));
        assertEquals(0, BufferUtil.posOnBuffer(buffers, 20));
        assertEquals(9, BufferUtil.posOnBuffer(buffers, 29));
    }

    @Test
    public void posOnBuffer_returns_minus_one_if_position_is_greater_than_total_buffer_size() {
        var buffer1 = ByteBuffer.allocate(10);
        var buffer2 = ByteBuffer.allocate(10);
        var buffer3 = ByteBuffer.allocate(10);
        var buffers = List.of(buffer1, buffer2, buffer3);

        assertEquals(-1, BufferUtil.posOnBuffer(buffers, 30));
    }

    @Test
    public void posOnBuffer_uneven_buffer_size() {
        var buffer1 = ByteBuffer.allocate(10);
        var buffer2 = ByteBuffer.allocate(2);
        var buffer3 = ByteBuffer.allocate(50);
        var buffers = List.of(buffer1, buffer2, buffer3);

        //buffer1
        assertEquals(0, BufferUtil.posOnBuffer(buffers, 0));
        assertEquals(9, BufferUtil.posOnBuffer(buffers, 9));

        //buffer2
        assertEquals(0, BufferUtil.posOnBuffer(buffers, 10));
        assertEquals(1, BufferUtil.posOnBuffer(buffers, 11));

        //buffer3
        assertEquals(0, BufferUtil.posOnBuffer(buffers, 12));
        assertEquals(49, BufferUtil.posOnBuffer(buffers, 61));
    }


}