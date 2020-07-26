package io.joshworks.es;

import io.joshworks.es.events.WriteEvent;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreTest {

    public static final int MEMTABLE_SIZE = 500;
    private EventStore store;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = open();
    }

    private EventStore open() {
        return new EventStore(root, Size.MB.ofInt(512), MEMTABLE_SIZE, 4096);
    }

    @Test
    public void append_get() {
        String stream = "abc-123";
        int items = (int) (MEMTABLE_SIZE * 1.5);

        for (int i = 0; i < items; i++) {
            store.append(create(stream, i - 1, "TEST", "abc"));
        }

        store.flushWrites();
        assertEventBatch(stream, 0, items);
    }

    @Test
    public void append_expected_version() {
        String stream = "stream-1";
        int entries = (int) (MEMTABLE_SIZE * 1.5);

        for (int i = 0; i < entries; i++) {
            store.append(create(stream, i - 1, "TEST", "abc"));
        }
    }

    @Test
    public void version() {
        String stream = "stream-1";
        int entries = (int) (MEMTABLE_SIZE * 2.5);

        assertEquals(-1, store.version(stream));

        for (int i = 0; i < entries; i++) {
            store.append(create(stream, i - 1, "TEST", "abc"));
            store.flushWrites();
            assertEquals(i, store.version(stream));
        }
    }

    @Test
    public void linkTo() {
        String srcStream = "stream-1";
        String dstStream = "stream-2";

        store.append(create(srcStream, -1, "TEST", "abc"));
        store.linkTo(srcStream, 0, dstStream, -1);

        store.flushWrites();

        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        store.get(dstStream, 0, readBuffer);

        readBuffer.flip();
        assertTrue(Event.isValid(readBuffer));
        assertEquals(StreamHasher.hash(dstStream), Event.stream(readBuffer));
        assertEquals(0, Event.version(readBuffer));
    }

    @Test
    public void restore() {
        String stream = "abc-123";
        int items = (int) (MEMTABLE_SIZE * 1.5);

        for (int i = 0; i < items; i++) {
            store.append(create(stream, -1, "CREATE", "abc"));
        }
        store.flushWrites();

        store.close();
        store = open();

        assertEventBatch(stream, 0, items);

    }

    public void assertEventBatch(String stream, int startVersion, int numEvents) {
        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        int version = startVersion;
        int events = 0;
        int read;
        do {
            read = store.get(stream, version, readBuffer.clear());
            readBuffer.flip();
            while (Event.isValid(readBuffer)) {
                int size = Event.sizeOf(readBuffer);
                assertTrue(Event.isValid(readBuffer));
                assertEquals(StreamHasher.hash(stream), Event.stream(readBuffer));
                assertEquals(version++, Event.version(readBuffer));

                System.out.println(Event.toString(readBuffer));

                Buffers.offsetPosition(readBuffer, size);
                events++;
            }
        } while (read > 0);

        assertEquals(numEvents, events);

    }

    private static WriteEvent create(String stream, int expectedVersion, String evType, String data) {
        WriteEvent event = new WriteEvent();
        event.stream = stream;
        event.expectedVersion = expectedVersion;
        event.type = evType;
        event.data = data.getBytes(StandardCharsets.UTF_8);
        event.metadata = new byte[0];

        return event;

    }

}