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

    public static final int MEMTABLE_SIZE = 500000;
    private EventStore store;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = open();
    }

    private EventStore open() {
        return new EventStore(root, Size.MB.ofInt(100), MEMTABLE_SIZE, 0.1, 4096);
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

    @Test
    public void append_expected_version() {
        long stream = 123;
        int entries = (int) (MEMTABLE_SIZE * 1.5);

        for (int i = 0; i < entries; i++) {
            store.append(stream, i - 1, wrap("abc"));
        }
    }

    @Test
    public void version() {
        long stream = 123;
        int entries = (int) (MEMTABLE_SIZE * 2.5);

        assertEquals(-1, store.version(stream));

        for (int i = 0; i < entries; i++) {
            store.append(stream, -1, wrap("abc"));
            assertEquals(i, store.version(stream));
        }
    }

    @Test
    public void linkTo() {
        long srcStream = 123;
        long dstStream = 456;

        store.append(srcStream, -1, wrap("abc"));
        store.linkTo(srcStream, 0, dstStream, -1);

        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        store.get(dstStream, 0, readBuffer);

        readBuffer.flip();
        assertTrue(Event.isValid(readBuffer));
        assertEquals(dstStream, Event.stream(readBuffer));
        assertEquals(0, Event.version(readBuffer));
    }

    @Test
    public void append_MANY_SAME_STREAM_TEST() {
        long stream = 123;
        int items = 20000000;

        ByteBuffer data = wrap("abc");

        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            if (i % 500000 == 0) {
                System.out.println();
            }
            store.append(stream, i - 1, data.clear());
            if (i % 1000000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("WRITE: " + i + " -> " + (now - s));
                s = now;
            }
        }

        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        for (int i = 0; i < items; i++) {
            int read = store.get(stream, i, readBuffer.clear());
            readBuffer.flip();

            assertTrue("Failed on " + i, read > 0);
            assertTrue(Event.isValid(readBuffer));
            assertEquals(stream, Event.stream(readBuffer));
            assertEquals(i, Event.version(readBuffer));
            if (i % 1000000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("READ: " + i + " -> " + (now - s));
                s = now;
            }
        }


    }


}