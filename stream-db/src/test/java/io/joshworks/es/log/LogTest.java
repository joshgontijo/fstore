package io.joshworks.es.log;

import io.joshworks.es.Event;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogTest {

    private Log log;
    private File testFolder;

    @Before
    public void setUp() {
        testFolder = TestUtils.testFolder();
        log = open();
    }

    public Log open() {
        return new Log(testFolder, Size.MB.ofInt(1));
    }

    @Test
    public void append_reopen() {

        ByteBuffer data = EventUtils.create(1, "stream-1", 1, "value-123");
        long address = log.append(data);

        log.close();
        log = open();

        assertEvent("stream-1", 1, 1, address);
    }

    @Test
    public void append_read() {

        String stream = "stream-1";
        int version = 111;
        long sequence = 222;
        ByteBuffer data = EventUtils.create(sequence, stream, version, "value-" + 0);
        long address = log.append(data);

        assertEvent(stream, version, sequence, address);
    }

    @Test
    public void append_multiple_segments_read() {

        long address1 = log.append(EventUtils.create(1, "stream-1", 1, "segment-1"));
        log.roll();
        long address2 = log.append(EventUtils.create(2, "stream-2", 2, "segment-1"));

        assertEvent("stream-1", 1, 1, address1);
        assertEvent("stream-2", 2, 2, address2);
    }

    private void assertEvent(String stream, int version, long sequence, long address) {
        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        int read = log.read(address, readBuffer);

        assertTrue(read > 0);

        readBuffer.flip();
        assertTrue(Event.isValid(readBuffer));
        assertEquals(stream, Event.stream(readBuffer));
        assertEquals(version, Event.version(readBuffer));
        assertEquals(sequence, Event.sequence(readBuffer));
    }

}