package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.StreamBlock;
import io.joshworks.es2.sstable.TestEvent;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreIT {

    private EventStore store;
    private File root;


    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = new EventStore(root.toPath(), Executors.newSingleThreadExecutor());
    }

    @After
    public void tearDown() {
        store.close();
    }

    @Test
    public void read() {

        int items = 5_000_000;
        String stream = "stream-1";
        TestEvent ev1 = TestEvent.create(stream, Event.NO_VERSION, "type-a", "data-1");
        ByteBuffer data = ev1.serialize();

        //write
        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            data.clear();
            Event.writeVersion(data, Event.NO_VERSION);
            store.append(data);

            if (i % 100_000 == 0) {
                System.out.println("WRITE: " + i + " - " + (System.currentTimeMillis() - s) + "ms");
                s = System.currentTimeMillis();
            }
        }

        store.compact().join();

        //read
        read(items, stream);
    }

    private void read(int items, String stream) {
        long s;
        System.out.println("READING");
        Sink.Memory sink = new Sink.Memory();
        int currVersion = 0;
        s = System.currentTimeMillis();
        int reads = 0;
        do {
            if (currVersion == 419430) {
                System.out.println();
            }
            int read = store.read(StreamHasher.hash(stream), currVersion, sink);
            assertTrue("Failed on " + currVersion, read > 0);

            ByteBuffer readData = ByteBuffer.wrap(sink.data());
            int startVersion = StreamBlock.startVersion(readData);
            int entries = StreamBlock.entries(readData);

            assertEquals(currVersion, startVersion);
            assertTrue(entries > 0);

            currVersion = startVersion + entries;
            sink.close(); //reset buffer
            reads++;

        } while (currVersion < items);

        long timeDiff = System.currentTimeMillis() - s;
        double avgTimePerRead = timeDiff / (double) reads;
        System.out.println("READ: " + timeDiff + " -> READS: " + reads + " AVG/R: " + format("%.2f", avgTimePerRead));
    }

}