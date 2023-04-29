package io.joshworks.es2;

import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.CompactionProfile;
import io.joshworks.es2.sstable.StreamBlock;
import io.joshworks.es2.utils.SSTableDump;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class EventStoreConcurrencyIT {

    private final AtomicInteger read = new AtomicInteger();
    private EventStore store;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        var builder = EventStore.builder();
        builder.memtable()
                .direct(false)
                .size(Size.MB.ofInt(100));
        builder.compaction()
                .threshold(3)
                .high(new CompactionProfile());

        builder.log()
                .flushMode(Builder.FlushMode.ON_WRITE);

        store = builder
                .open(root.toPath());
    }

    @After
    public void tearDown() {
        store.close();
        TestUtils.deleteRecursively(root);
    }

    @Test
    public void concurrent() throws Exception {

        int items = 10_000_000;
        long stream = 123L;
        int writers = 5;
        int readers = 20;

        var threads = new ArrayList<Thread>();
        for (int i = 0; i < writers; i++) {
            threads.add(Threads.spawn("writer-" + i, () -> write(items, stream)));
        }
        for (int i = 0; i < readers; i++) {
            threads.add(Threads.spawn("reader-" + i, () -> read(items * writers, stream)));
        }

        Threads.waitFor(threads);

        store.compact().join();

        SSTableDump.dumpStream(stream, store, new File("out-" + stream + ".txt"));




    }

    private void write(int items, long stream) {
        for (int i = 0; i < items; i++) {
            ByteBuffer data = Event.create(stream, Event.NO_VERSION, "type-a", (Thread.currentThread().getId() + "_data-" + i).getBytes(StandardCharsets.UTF_8));
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