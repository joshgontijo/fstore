package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ConcurrencyIT {

    private File directory;
    private IEventStore store;

    @Before
    public void setUp() {
        directory = FileUtils.testFolder();
        store = EventStore.open(directory);
//        store = new QueuedEventStore(EventStore.open(directory));
    }

    @After
    public void tearDown() {
        store.close();
        FileUtils.tryDelete(new File(directory, "index"));
        FileUtils.tryDelete(new File(directory, "projections"));
        FileUtils.tryDelete(directory);
    }

    @Test
    public void concurrent_write_same_stream() throws InterruptedException {

        int threads = 10;
        int itemPerThread = 500000;
        String stream = "stream-0";
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        CountDownLatch latch = new CountDownLatch(threads);
        for (int thread = 0; thread < threads; thread++) {
            executor.execute(() -> {
                TimeWatch watch = TimeWatch.start();
                for (int i = 0; i < itemPerThread; i++) {
                    store.append(EventRecord.create(stream, "" + i, "data-" + i));
                }
                latch.countDown();
                System.out.println("Thread " + Thread.currentThread().getName() + " took " + watch.time() + " to write " + itemPerThread + " entries");
            });
        }

        executor.shutdown();
        if (!latch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }


        //READ
        Iterator<EventRecord> events = store.fromStreamIter(stream);
        int found = 0;

        while (events.hasNext()) {
            EventRecord next = events.next();
            found++;
        }

        assertEquals(threads * itemPerThread, found);
    }


    @Test
    public void concurrent_write_thread_per_stream() throws InterruptedException {

        int threads = 50;
        int itemPerThread = 100000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        Set<String> streamNames = new HashSet<>();
        CountDownLatch latch = new CountDownLatch(threads);
        for (int thread = 0; thread < threads; thread++) {
            executor.execute(() -> {
                String threadName = Thread.currentThread().getName();
                streamNames.add(threadName);

                TimeWatch watch = TimeWatch.start();
                for (int i = 0; i < itemPerThread; i++) {
                    store.append(EventRecord.create(threadName, "" + i, "data-" + i));
                }
                latch.countDown();
                System.out.println("Thread " +threadName + " took " + watch.time() + " to write " + itemPerThread + " entries");
            });
        }


        executor.shutdown();
        if (!latch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }


        //READ
        Iterator<EventRecord> events = store.zipStreamsIter(streamNames);
        int found = 0;

        while (events.hasNext()) {
            EventRecord next = events.next();
            found++;
        }

        assertEquals(threads * itemPerThread, found);
    }


}
