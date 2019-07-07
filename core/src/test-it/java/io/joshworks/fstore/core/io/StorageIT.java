package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.fstore.core.io.Storage.EOF;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;

public abstract class StorageIT {

    private static final long STORAGE_SIZE = Size.GB.of(2);
    private Storage storage;
    private File testFile;

    protected abstract Storage store(File file, long size);

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        storage = store(testFile, STORAGE_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        Utils.tryDelete(testFile);
    }

    @Test
    public void write_read_8GB() {
        byte[] data = fillWithUniqueBytes();

        long written = 0;
        long entries = 0;
        long total = Size.GB.of(8);
        while (written < total) {
            written += storage.write(ByteBuffer.wrap(data));
            if (entries++ % 1000000 == 0) {
                System.out.println("Written: " + written + "/" + total);
            }
        }
        System.out.println("Written: " + written + "/" + total);

        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        long totalRead = 0;
        entries = 0;
        do {
            int read = storage.read(totalRead, readBuffer);
            readBuffer.flip();
            assertArrayEquals("Failed response assertion on position " + totalRead, data, readBuffer.array());
            readBuffer.clear();
            totalRead += read;
            if (entries++ % 1000000 == 0) {
                System.out.println("Read: " + totalRead + "/" + written);
            }

        } while (totalRead < written);
        System.out.println("Read: " + totalRead + "/" + written);
    }

    @Test
    public void must_support_concurrent_reads_and_writes() throws InterruptedException {

        int items = 5000000;

        byte[] data = fillWithUniqueBytes();

        final AtomicBoolean done = new AtomicBoolean();
        final AtomicBoolean writeFailed = new AtomicBoolean();
        final AtomicBoolean readFailed = new AtomicBoolean();
        try {
            Thread writer = new Thread(() -> {
                long entries = 0;
                for (int i = 0; i < items; i++) {
                    try {
                        storage.write(ByteBuffer.wrap(data));
                        if (entries++ % 1000000 == 0) {
                            System.out.println("WRITE: " + entries + "/" + items);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        writeFailed.set(true);
                        break;
                    }
                    if (readFailed.get() || writeFailed.get()) {
                        break;
                    }
                }
                done.set(true);
            });

            Runnable task = () -> {
                long readPos = 0;
                int read;
                long count = 0;
                do {
                    try {
                        var readBuffer = ByteBuffer.allocate(data.length);
                        read = storage.read(readPos, readBuffer);
                        readBuffer.flip();
                        if (read != EOF && !Arrays.equals(data, readBuffer.array())) {
                            System.err.println("POSITION: " + readPos);
                            System.err.println("EXPECTED: " + Arrays.toString(data));
                            System.err.println("FOUND   : " + Arrays.toString(readBuffer.array()));
                            readFailed.set(true);
                        }
                        readPos += read == EOF ? 0 : read;
                        if (read == EOF) {
                            Threads.sleep(2000);
                        }

                        if (read != EOF && count++ % 1000000 == 0) {
                            System.out.println("[" + Thread.currentThread().getName() + "] READ: " + count);
                        }

                    } catch (Exception e) {
                        e.printStackTrace();
                        readFailed.set(true);
                        break;
                    }
                } while (count < items && !readFailed.get() && !writeFailed.get());
            };

            writer.start();

            int readTasks = 50;
            int concurrentReads = 10;
            ExecutorService executor = Executors.newFixedThreadPool(concurrentReads);

            for (int i = 0; i < readTasks; i++) {
                executor.submit(task);
            }

            executor.shutdown();
            writer.join();
            System.out.println("Waiting read tasks");
            executor.awaitTermination(1, TimeUnit.HOURS);


            assertFalse(writeFailed.get());
            assertFalse(readFailed.get());
        } finally {
            IOUtils.closeQuietly(storage);
        }

    }

    private byte[] fillWithUniqueBytes() {
        int entrySize = 255;
        byte[] data = new byte[entrySize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        return data;
    }

    public static class RafStorageTest extends StorageIT {

        @Override
        protected Storage store(File file, long length) {
            return new DiskStorage(file, length, IOUtils.randomAccessFile(file, length));
        }
    }

    public static class MMapCacheTest extends StorageIT {

        @Override
        protected Storage store(File file, long length) {
            return new MMapCache(new DiskStorage(file, length, IOUtils.randomAccessFile(file, length)));
        }
    }

    public static class MMapStorageTest extends StorageIT {

        @Override
        protected Storage store(File file, long length) {
            return new MMapStorage(new DiskStorage(file, length, IOUtils.randomAccessFile(file, length)));
        }
    }

    public static class OffHeapStorageTest extends StorageIT {

        @Override
        protected Storage store(File file, long size) {
            return new OffHeapStorage(file.getName(), size);
        }
    }

    public static class HeapStorageTest extends StorageIT {

        @Override
        protected Storage store(File file, long size) {
            return new HeapStorage(file.getName(), size);
        }
    }

}
