package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.StreamBlock;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreConcurrencyIT {

    private EventStore store;
    private File root;

    private final AtomicInteger read = new AtomicInteger();

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = new EventStore(root.toPath(), Executors.newSingleThreadExecutor());
    }

    @After
    public void tearDown() {
        store.close();
        TestUtils.deleteRecursively(root);
    }

    @Test
    public void concurrent() {

        int items = 5_000_000;
        long stream = 123L;


        var writer = Threads.spawn("writer", () -> write(items, stream));
        var reader = Threads.spawn("reader", () -> read(items, stream));

        Threads.waitFor(writer, reader);
    }

    private void write(int items, long stream) {
        for (int i = 0; i < items; i++) {
            ByteBuffer data = Event.create(stream, Event.NO_VERSION, "type-a", "data-1".getBytes(StandardCharsets.UTF_8));
            Event.writeVersion(data, Event.NO_VERSION);
            store.append(data);

            if (i % 100_000 == 0) {
                System.out.println("WRITE: " + i + "  READ: " + read.get());
            }
        }
    }


    private void read(int items, long stream) {
        Sink.Memory sink = new Sink.Memory();
        int currVersion = 0;
        int totalEntries = 0;
        do {
            int read = store.read(stream, currVersion, sink);
            if (read <= 0) {
                Threads.sleep(1);
                continue;
            }

            ByteBuffer readData = ByteBuffer.wrap(sink.data());
            int startVersion = StreamBlock.startVersion(readData);
            int entries = StreamBlock.entries(readData);
            int endVersion = startVersion + entries;


            assertTrue(entries > 0);
            assertTrue(currVersion >= startVersion); //read stream may return a lower version
            assertTrue(currVersion < endVersion);

            totalEntries += endVersion - currVersion;
            this.read.set(totalEntries);


            currVersion = endVersion;
            sink.close(); //reset buffer

        } while (currVersion < items);

        assertEquals(items, totalEntries);
    }

}