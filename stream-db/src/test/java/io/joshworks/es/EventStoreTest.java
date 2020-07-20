package io.joshworks.es;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.wrap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreTest {

    private EventStore store;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = open();
    }

    private EventStore open() {
        return new EventStore(root, Size.MB.ofInt(1), 100);
    }

    @Test
    public void append_get() {
        long stream = 123;

        store.append(stream, -1, wrap("abc"));
        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        int read = store.get(stream, 0, readBuffer);
        readBuffer.flip();

        assertTrue(read > 0);
        assertTrue(Event.isValid(readBuffer));
        assertEquals(stream, Event.stream(readBuffer));
        assertEquals(0, Event.version(readBuffer));


    }


}