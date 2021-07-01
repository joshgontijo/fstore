package io.joshworks.es2;

import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SegmentChannelTest {

    private File testFile;
    private SegmentChannel channel;

    @Before
    public void setUp() {
        testFile = TestUtils.testFile();
        channel = SegmentChannel.create(testFile);
    }

    @After
    public void tearDown() throws Exception {
        channel.delete();
    }

    @Test
    public void name() {

        var b = ByteBuffer.allocate(Integer.BYTES * 2); //len + data
        for (int i = 0; i < 100_000; i++) {
            channel.append(b.clear()
                    .putInt(Integer.BYTES * 2)
                    .putInt(i)
                    .flip());
        }

        var it = new LengthPrefixedChannelIterator(channel);

        for (int i = 0; i < 100_000; i++) {
            assertTrue(it.hasNext());
            var data = it.next();
            assertEquals("Failed on " + i, Integer.BYTES * 2, data.remaining());
        }


    }
}