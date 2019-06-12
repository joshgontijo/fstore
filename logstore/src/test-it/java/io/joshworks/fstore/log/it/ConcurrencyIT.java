package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public abstract class ConcurrencyIT {

    private LogAppender<String> appender;

    protected abstract LogAppender<String> appender(File testDirectory);

    private File testDirectory;

    private static final long SEGMENT_SIZE = Size.MB.of(5);

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

    //TODO this might hang since hasNext will return null when not all entries are present in the logs
    @Test
    public void full_scan() throws InterruptedException {

        int parallelReads = 50;
        int totalItems = 20000000;
        ExecutorService executor = Executors.newFixedThreadPool(parallelReads);

        AtomicInteger completedTasks = new AtomicInteger();
        AtomicBoolean writeFailed = new AtomicBoolean();
        AtomicLong failed = new AtomicLong();
        AtomicLong reads = new AtomicLong();
        AtomicLong writes = new AtomicLong();

        Thread writeThread = new Thread(() -> {
            try {
                int counter = 0;
                do {
                    appender.append(String.valueOf(counter++));
                } while (writes.incrementAndGet() < totalItems);

            } catch (Exception e) {
                writeFailed.set(true);
            }
        });
        writeThread.start();

        Thread thread = new Thread(() -> {
            while (writeThread.isAlive() || !executor.isShutdown()) {
                System.out.println("READ TASKS COMPLETED: " + completedTasks.get() + " | WRITES: " + writes.get() + " | READS: " + reads.get() + " | FAILED: " + failed.get());
            }

        });


        for (int i = 0; i < parallelReads; i++) {
            executor.execute(() -> {
                String lastEntry = null;
                try (LogIterator<String> iterator = appender.iterator(Direction.FORWARD)) {
                    for (int j = 0; j < totalItems; j++) {
                        while (!writeFailed.get() && !iterator.hasNext()) {
                            Thread.sleep(100);
                        }
                        if (writeFailed.get()) {
                            break;
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
        Threads.awaitTerminationOf(executor, 10, TimeUnit.MINUTES);

        System.out.println("Completed");
        System.out.println("TASKS COMPLETED: " + completedTasks.get() + " | WRITES: " + writes.get() + " | READS: " + reads.get() + " | FAILED: " + failed.get());

        assertFalse(writeFailed.get());
        assertEquals(0, failed.get());
        assertEquals(totalItems, writes.get());
        assertEquals(parallelReads * totalItems, reads.get());
    }

    @Test
    public void writes() throws InterruptedException {

        int writeItems = 10000000;

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
        Thread writeThread = new Thread(() -> {
            for (int i = 0; i < writeItems; i++) {
                try {
                    appender.append(String.valueOf(writes.getAndIncrement()));
                } catch (Exception e) {
                    failed.incrementAndGet();
                    e.printStackTrace();
                } finally {
                    completedTasks.incrementAndGet();
                }
            }
        });
        writeThread.start();
        writeThread.join();

        System.out.println("COMPLETED IN " + watch.time());
        System.out.println("TASKS COMPLETED: " + completedTasks.get() + " | FAILED: " + failed.get());

        assertEquals(0, failed.get());
    }

    @Test
    public void compaction_can_run_in_parallel_with_reads_and_writes() throws InterruptedException {

        int scans = 10;
        int items = 20000000;
        for (int i = 0; i < items; i++) {
            appender.append(String.valueOf(i));
        }

        AtomicBoolean failed = new AtomicBoolean();
        Thread read = new Thread(() -> {
            for (int i = 0; i < scans; i++) {
                System.out.println("Scanning " + i + "/" + scans);
                LogIterator<String> iterator = appender.iterator(Direction.FORWARD);
                int val = 0;
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    String expected = String.valueOf(val++);
                    if (!expected.equals(next)) {
                        failed.set(true);
                        assertEquals(expected, next);
                    }
                }
                int expectedTotal = val - 1;
                if (items != expectedTotal) {
                    failed.set(true);
                    assertEquals(items, expectedTotal);
                }
            }
        });

        appender.compact();
        read.start();
        read.join();

        assertFalse("Read failed, check logs", failed.get());

        appender.close(); //close will wait for compaction
        appender = appender(testDirectory);
    }


    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static class RafIT extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF)
                    .open();
        }
    }

    public static class MMapIT extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.MMAP)
                    .open();
        }
    }

    public static class CachedRafIT extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF_CACHED)
                    .open();
        }
    }

    public static class OffHeapIT extends ConcurrencyIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.OFF_HEAP)
                    .open();
        }
    }


}
