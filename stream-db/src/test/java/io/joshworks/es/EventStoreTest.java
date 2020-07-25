package io.joshworks.es;

import io.joshworks.es.events.WriteEvent;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreTest {

    public static final int MEMTABLE_SIZE = 1000000;
    private EventStore store;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = open();
    }

    private EventStore open() {
        return new EventStore(root, Size.MB.ofInt(512), MEMTABLE_SIZE, 4096, 1000);
    }

    @Test
    public void append_get() {
        String stream = "abc-123";

        store.append(create(stream, -1, "CREATE", "abc"));
        store.append(create(stream, -1, "UPDATE", "123"));
        store.append(create(stream, -1, "UPDATE", "123"));
        store.append(create(stream, -1, "UPDATE", "123"));
        store.append(create(stream, -1, "UPDATE", "123"));
        store.append(create(stream, -1, "UPDATE", "123"));

        Threads.sleep(2000);
        store.flush();

        store.append(create(stream, -1, "UPDATE", "123"));
        store.append(create(stream, -1, "UPDATE", "123"));
        store.append(create(stream, -1, "UPDATE", "123"));

        Threads.sleep(2000);


        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        int read = store.get(stream, 0, readBuffer);
        readBuffer.flip();

        int version = 0;
        while (Event.isValid(readBuffer)) {
            int size = Event.sizeOf(readBuffer);
            assertTrue(read > 0);
            assertTrue(Event.isValid(readBuffer));
            assertEquals(StreamHasher.hash(stream), Event.stream(readBuffer));
            assertEquals(version++, Event.version(readBuffer));

            Buffers.offsetPosition(readBuffer, size);
        }


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

    //    @Test
//    public void append_expected_version() {
//        long stream = 123;
//        int entries = (int) (MEMTABLE_SIZE * 1.5);
//
//        for (int i = 0; i < entries; i++) {
//            store.append(stream, i - 1, wrap("abc"));
//        }
//    }
//
//    @Test
//    public void version() {
//        long stream = 123;
//        int entries = (int) (MEMTABLE_SIZE * 2.5);
//
//        assertEquals(-1, store.version(stream));
//
//        for (int i = 0; i < entries; i++) {
//            store.append(stream, -1, wrap("abc"));
//            assertEquals(i, store.version(stream));
//        }
//    }
//
    @Test
    public void linkTo() {
        String srcStream = "stream-1";
        String dstStream = "stream-2";

        store.append(create(srcStream, -1, "TEST", "abc"));
        store.linkTo(srcStream, 0, dstStream, -1);

        Threads.sleep(2000);

        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        store.get(dstStream, 0, readBuffer);

        readBuffer.flip();
        assertTrue(Event.isValid(readBuffer));
        assertEquals(StreamHasher.hash(dstStream), Event.stream(readBuffer));
        assertEquals(0, Event.version(readBuffer));
    }

    @Test
    public void append_MANY_SAME_STREAM_TEST() {
        String stream = "stream-1";
        int items = 30000000;

        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            store.append(create(stream, i - 1, "TEST", "abc"));
            if (i % 1000000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("WRITE: " + i + " -> " + (now - s));
                s = now;
            }
        }

        Threads.sleep(5000);
        s = System.currentTimeMillis();


        int totalItems = 0;
        int read;
        int lastReadVersion = -1;
        ByteBuffer readBuffer = Buffers.allocate(4096, false);
        do {
            readBuffer.clear();
            read = store.get(stream, lastReadVersion + 1, readBuffer);
            readBuffer.flip();

            while (Event.isValid(readBuffer)) {
                int size = Event.sizeOf(readBuffer);
                assertTrue(Event.isValid(readBuffer));
                assertEquals(StreamHasher.hash(stream), Event.stream(readBuffer));
                assertEquals(lastReadVersion + 1, Event.version(readBuffer));
                lastReadVersion++;
                totalItems++;
                Buffers.offsetPosition(readBuffer, size);

                if (totalItems % 1000000 == 0) {
                    long now = System.currentTimeMillis();
                    System.out.println("READ: " + totalItems + " -> " + (now - s));
                    s = now;
                }
            }
        } while (read > 0);

    }


}