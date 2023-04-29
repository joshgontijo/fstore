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
import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreIT {

    private EventStore store;
    private File root;


    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        store = EventStore.open(root.toPath()).build();
    }

    @After
    public void tearDown() {
        store.close();
        TestUtils.deleteRecursively(root);
    }

    @Test
    public void read() {

        int items = 5_000_000;
        long stream = 123L;


        //write
        var future = CompletableFuture.completedFuture(-1);
        long s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            ByteBuffer data = Event.create(stream, Event.NO_VERSION, "type-a", "data-1".getBytes(StandardCharsets.UTF_8));
            Event.writeVersion(data, Event.NO_VERSION);
            future.thenCombine(store.append(data), Math::max);

            if (i % 100_000 == 0) {
                System.out.println("WRITE: " + i + " - " + (System.currentTimeMillis() - s) + "ms");
                s = System.currentTimeMillis();
            }
        }

        future.join();
        store.compact().join();

        //read
        read(items, stream);
    }

    private void read(int items, long stream) {
        long s;
        Threads.sleep(5000);
        System.out.println("READING");
        Sink.Memory sink = new Sink.Memory();
        int currVersion = 0;
        s = System.currentTimeMillis();
        int totalEntries = 0;
        int reads = 0;
        do {
            int read = store.read(stream, currVersion, sink);
            assertTrue("Failed on " + currVersion, read > 0);

            ByteBuffer readData = ByteBuffer.wrap(sink.data());
            int startVersion = StreamBlock.startVersion(readData);
            int entries = StreamBlock.entries(readData);

            totalEntries += entries;
            assertEquals(currVersion, startVersion);
            assertTrue(entries > 0);

            currVersion = startVersion + entries;
            sink.close(); //reset buffer
            reads++;

        } while (currVersion < items);

        long timeDiff = System.currentTimeMillis() - s;
        double avgTimePerRead = timeDiff / (double) reads;
        System.out.println("READ: " + totalEntries + " in " + timeDiff + "ms -> READS: " + reads + " AVG/R: " + format("%.2f", avgTimePerRead));

        assertEquals(items, totalEntries);
    }

}