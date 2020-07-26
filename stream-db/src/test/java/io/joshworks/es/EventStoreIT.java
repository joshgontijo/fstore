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

public class EventStoreIT {

    public static final int MEMTABLE_SIZE = 100;
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
    public void append_read() {
        String stream = "stream-1";
        int items = 3000000;

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