package io.joshworks.es2;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LengthPrefixedChannelIteratorTest {

    private SegmentChannel channel;

    @Before
    public void setUp() {
        channel = SegmentChannel.create(TestUtils.testFile());
    }

    @After
    public void tearDown() {
        channel.delete();
    }

    @Test
    public void read() {
        int recSize = Integer.BYTES * 2; //len + int value
        int writeBytes = Size.MB.ofInt(3);
        int entries = writeBytes / recSize;

        for (int i = 0; i < entries; i++) {
            channel.append(ByteBuffer.allocate(8)
                    .putInt(recSize) //len
                    .putInt(i) //val
                    .flip()
            );
        }

        var it = new LengthPrefixedChannelIterator(channel);
        for (int i = 0; i < entries; i++) {
            assertTrue(it.hasNext());
            ByteBuffer entry = it.next();
            assertEquals(recSize, entry.getInt());
            assertEquals(i, entry.getInt());
        }
    }

    @Test
    public void read_buffer_resize() {
        var d = new byte[8192];
        Arrays.fill(d, (byte) 1);

        int recLen = Integer.BYTES + d.length;
        channel.append(ByteBuffer.allocate(recLen)
                .putInt(recLen) //len
                .put(d) //val
                .flip());

        var it = new LengthPrefixedChannelIterator(channel);

        assertTrue(it.hasNext());
        var next = it.next();
        assertEquals(d.length, next.remaining() - Integer.BYTES); //minus LEN_LEN

        next.getInt(); //minus LEN_LEN
        assertArrayEquals(d, Buffers.copyArray(next));
    }

}