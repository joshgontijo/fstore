package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;

public abstract class ConcurrencyIT {

    private LogAppender<String> appender;

    protected abstract LogAppender<String> appender(File testDirectory);

    private File testDirectory;

    private static final int SEGMENT_SIZE_MB = 5;

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        testDirectory.deleteOnExit();
        appender = appender(testDirectory);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void full_scan() throws InterruptedException {

        int readThreads = 50;
        int tasks = 1000;
        int writeItems = 20000000;
        ExecutorService executor = Executors.newFixedThreadPool(readThreads);

        AtomicInteger completedTasks = new AtomicInteger();
        AtomicLong failed = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        AtomicLong writes = new AtomicLong();


        Thread writeThread = new Thread(() -> {
            int counter = 0;
            do {
                appender.append(String.valueOf(counter++));
            } while (writes.incrementAndGet() < writeItems);
        });
        writeThread.start();


        Thread reportThread = new Thread(() -> {
            while (completedTasks.get() < tasks) {
                System.out.println("TASKS COMPLETED: " + completedTasks.get() + " | WRITES: " + writes.get() + " | READS: " + reads.get() + " | FAILED: " + failed.get());
                sleep(2000);
            }
        });
        reportThread.start();


        sleep(10000);

        for (int i = 0; i < tasks; i++) {
            executor.execute(() -> {
                String lastEntry = null;
                try (LogIterator<String> iterator = appender.iterator(Direction.FORWARD)) {
                    for (int j = 0; j < writeItems; j++) {
                        while (!iterator.hasNext()) {
                            Thread.sleep(1000);
                        }
                        String next = iterator.next();
                        reads.incrementAndGet();

                        if (lastEntry == null) {
                            lastEntry = next;
                            continue;
                        }

                        long prev = Long.parseLong(lastEntry);
                        long curr = Long.parseLong(next);
                        assertEquals(prev + 1, curr);
                        lastEntry = next;
                    }

                } catch (Exception e) {
                    failed.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    completedTasks.incrementAndGet();
                }
            });
        }


        writeThread.join();
        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.HOURS);

        System.out.println("Completed");
        System.out.println("TASKS COMPLETED: " + completedTasks.get() + " | WRITES: " + writes.get() + " | READS: " + reads.get() + " | FAILED: " + failed.get());

        assertEquals(0, failed.get());

    }

    @Test
    public void writes() throws InterruptedException {

        int threads = 50;
        int writeItems = 10000000;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        AtomicInteger completedTasks = new AtomicInteger();
        AtomicLong failed = new AtomicLong();
        AtomicLong writes = new AtomicLong();

        Thread reportThread = new Thread(() -> {
            while (completedTasks.get() < writeItems) {
                System.out.println("TASKS COMPLETED: " + completedTasks.get() + " | FAILED: " + failed.get());
                sleep(2000);
            }
        });
        reportThread.start();

        TimeWatch watch = TimeWatch.start();
        for (int i = 0; i < writeItems; i++) {
            executor.execute(() -> {
                try  {
                    appender.append(String.valueOf(writes.getAndIncrement()));
                } catch (Exception e) {
                    failed.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    completedTasks.incrementAndGet();
                }
            });
        }




        executor.shutdown();
        executor.awaitTermination(2, TimeUnit.HOURS);

        System.out.println("COMPLETED IN " + watch.time());
        System.out.println("TASKS COMPLETED: " + completedTasks.get() + " | FAILED: " + failed.get());

        assertEquals(0, failed.get());

    }


    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static class RafConcurrencyTest extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(Size.MB.of(SEGMENT_SIZE_MB))
                    .storageMode(StorageMode.RAF)
                    .disableCompaction()
                    .open();
        }
    }

    public static class MMapConcurrencyTest extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(Size.MB.of(SEGMENT_SIZE_MB))
                    .storageMode(StorageMode.MMAP)
                    .open();
        }
    }

    public static class CachedRafConcurrencyTest extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(Size.MB.of(SEGMENT_SIZE_MB))
                    .storageMode(StorageMode.MMAP)
                    .open();
        }
    }


}
