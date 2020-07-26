package io.joshworks.es;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static io.joshworks.es.EventHelper.evOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreIT {

    public static final int MEMTABLE_SIZE = 500000;
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
        int items = 30000000;

        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            store.append(evOf(stream, i - 1, "TEST", "abc"));
            if (i % 1000000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println("WRITE: " + i + " -> " + (now - s));
                s = now;
            }
        }

        System.out.println("Reading");
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