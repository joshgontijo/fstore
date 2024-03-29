package io.joshworks.fstore.it;

import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.EventStore;
import io.joshworks.fstore.api.IEventStore;
import io.joshworks.fstore.api.EventStoreIterator;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collector;

import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrencyIT {

    private File directory;
    private IEventStore store;

    @Before
    public void setUp() {
        directory = TestUtils.testFolder();
        store = EventStore.open(directory);
//        store = new QueuedEventStore(EventStore.open(directory));
    }

    @After
    public void tearDown() {
        store.close();
        TestUtils.deleteRecursively(directory);
    }

    @Test
    public void concurrent_write_same_stream() throws InterruptedException {

        int threads = 20;
        int itemsPerThread = 500000;
        String stream = "stream-0";
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        final AtomicInteger writeCount = new AtomicInteger();
        for (int thread = 0; thread < threads; thread++) {
            executor.execute(() -> {
                for (int written = 0; written < itemsPerThread; written++) {
                    int i = writeCount.getAndIncrement();
                    store.append(EventRecord.create(stream, "" + i, Map.of()));
                }
            });
        }

        Thread reportThread = new Thread(() -> {
            while (writeCount.get() < itemsPerThread * threads) {
                System.out.println("WRITES: " + writeCount.get());
                sleep(2000);
            }
        });
        reportThread.start();

        executor.shutdown();
        reportThread.join();


        //READ
        Iterator<EventRecord> events = store.fromStream(EventId.parse(stream));
        int found = 0;

        while (events.hasNext()) {
            EventRecord next = events.next();
            found++;
        }

        assertEquals(itemsPerThread * threads, found);
    }

    @Test
    public void concurrent_write_read() throws InterruptedException {

        int writeThreads = 10;
        int totalWrites = 5000000;
        int readThreads = 20;
        String stream = "stream-0";
        ExecutorService writeExecutor = Executors.newFixedThreadPool(writeThreads);
        ExecutorService readExecutor = Executors.newFixedThreadPool(readThreads);

        CountDownLatch writeLatch = new CountDownLatch(totalWrites);
        CountDownLatch readLatch = new CountDownLatch(readThreads);

        final AtomicInteger writeCount = new AtomicInteger();
        final AtomicInteger reads = new AtomicInteger();

        Thread reportThread = new Thread(() -> {
            while (writeCount.get() < totalWrites || readLatch.getCount() > 0) {
                System.out.println("WRITES: " + writeCount.get() + " | READS: " + reads.get());
                sleep(2000);
            }
        });
        reportThread.start();

        Runnable writer = () -> write(store, stream, writeLatch, writeCount);
        for (int writeItem = 0; writeItem < totalWrites; writeItem++) {
            writeExecutor.execute(writer);
        }

        for (int readTask = 0; readTask < readThreads; readTask++) {
            readExecutor.execute(() -> {
                AtomicInteger localCounter = new AtomicInteger();
                try (EventStoreIterator iterator = store.fromStream(EventId.parse(stream))) {
                    while (localCounter.get() < totalWrites) {
                        while (!iterator.hasNext()) {
                            sleep(1000);
                        }
                        EventRecord next = iterator.next();
                        assertNotNull(next);
                        reads.incrementAndGet();
                        localCounter.incrementAndGet();
                    }
                    System.out.println("COMPLETED READING " + localCounter.get() + ": " + Thread.currentThread().getName());
                    readLatch.countDown();
                }


            });
        }

        writeExecutor.shutdown();
        readExecutor.shutdown();
        if (!writeLatch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }
        if (!readLatch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }

        assertEquals(readThreads * totalWrites, reads.get());

    }

    private static void write(IEventStore store, String stream, CountDownLatch writeLatch, AtomicInteger writeCount) {
        int id = writeCount.getAndIncrement();
        store.append(EventRecord.create(stream, "" + id, Map.of()));
        writeLatch.countDown();
    }

    @Test
    public void concurrent_write_thread_per_stream() throws InterruptedException, IOException {

        final int threads = 10;
        final int itemPerThread = 100000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        AtomicInteger written = new AtomicInteger();
        Set<String> streamNames = new HashSet<>();
        CountDownLatch latch = new CountDownLatch(threads);
        for (int thread = 0; thread < threads; thread++) {
            executor.execute(() -> {
                String threadName = Thread.currentThread().getName();
                streamNames.add(threadName);

                TimeWatch watch = TimeWatch.start();
                for (int i = 0; i < itemPerThread; i++) {
                    store.append(EventRecord.create(threadName, "type", Map.of()));
                    written.incrementAndGet();
                }
                latch.countDown();
                System.out.println("Thread " + threadName + " took " + watch.time() + " to write " + itemPerThread + " entries");
            });
        }

        Thread reportThread = new Thread(() -> {
            while (written.get() < threads * itemPerThread) {
                System.out.println("WRITES: " + written.get());
                sleep(2000);
            }
        });
        reportThread.start();

        executor.shutdown();
        if (!latch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }
        System.out.println("TOTAL WRITE: " + written.get());

        //READ

        Map<String, AtomicInteger> counter = new HashMap<>();
        EventMap eventMap = streamNames.stream().map(EventId::parse).collect(Collector.of(EventMap::empty, EventMap::add, EventMap::merge));
        try (EventStoreIterator events = store.fromStreams(eventMap)) {
            for (int i = 0; i < itemPerThread * threads; i++) {
                assertTrue("Failed on " + i, events.hasNext());
                EventRecord event = events.next();

                counter.putIfAbsent(event.stream, new AtomicInteger(NO_VERSION));
                AtomicInteger version = counter.get(event.stream);
                assertEquals(version.get() + 1, event.version);
                version.set(event.version);
            }
        }

    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

}
