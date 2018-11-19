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
import java.util.concurrent.atomic.AtomicInteger;

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

        int threads = 20;
        int totalItems = 15000000;
        String stream = "stream-0";
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        final AtomicInteger writeCount = new AtomicInteger();
        for (int written = 0; written < totalItems; written++) {
            executor.execute(() -> {
                int i = writeCount.getAndIncrement();
                store.append(EventRecord.create(stream, "" + i, "body-" + i));
            });
        }

        Thread reportThread = new Thread(() -> {
            while (writeCount.get() < totalItems) {
                System.out.println("WRITES: " + writeCount.get());
                sleep(2000);
            }
        });
        reportThread.start();

        executor.shutdown();
        reportThread.join();


        //READ
        Iterator<EventRecord> events = store.fromStream(stream);
        int found = 0;

        while (events.hasNext()) {
            EventRecord next = events.next();
            found++;
        }

        assertEquals(totalItems, found);
    }

    @Test
    public void concurrent_write_read() throws InterruptedException {

        int writeThreads = 10;
        int totalWrites = 1500000;
        int readThreads = 10;
        int totalReads = 10000;
        String stream = "stream-0";
        ExecutorService writeExecutor = Executors.newFixedThreadPool(writeThreads);
        ExecutorService readExecutor = Executors.newFixedThreadPool(readThreads);

        CountDownLatch writeLatch = new CountDownLatch(totalWrites);
        CountDownLatch readLatch = new CountDownLatch(totalWrites);

        final AtomicInteger writeCount = new AtomicInteger();
        final AtomicInteger readCount = new AtomicInteger();
        for (int writeItem = 0; writeItem < totalWrites; writeItem++) {
            writeExecutor.execute(() -> {
                int id = writeCount.getAndIncrement();
                store.append(EventRecord.create(stream, "" + id, "body-" + id));
                writeLatch.countDown();
            });
        }

        for (int readTask = 0; readTask < totalReads; readTask++) {
            readExecutor.execute(() -> {
                long count = store.fromStream(stream).stream().count();
                readLatch.countDown();
                readCount.incrementAndGet();
            });
        }

        Thread reportThread = new Thread(() -> {
            while (writeCount.get() < totalWrites) {
                System.out.println("WRITES: " + writeCount.get() + " | READS: " + readCount.get());
                sleep(2000);
            }
        });
        reportThread.start();


        writeExecutor.shutdown();
        readExecutor.shutdown();
        if (!writeLatch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }
        if (!readLatch.await(1, TimeUnit.HOURS)) {
            fail("Failed to write all entries");
        }
    }


    @Test
    public void concurrent_write_thread_per_stream() throws InterruptedException {

        int threads = 50;
        int itemPerThread = 100000;
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
                    store.append(EventRecord.create(threadName, "" + i, "body-" + i));
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


        //READ
        Iterator<EventRecord> events = store.zipStreams(streamNames);
        int found = 0;

        while (events.hasNext()) {
            EventRecord next = events.next();
            found++;
        }

        assertEquals(threads * itemPerThread, found);
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
