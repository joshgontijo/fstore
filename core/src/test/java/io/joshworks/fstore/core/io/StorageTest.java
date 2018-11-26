package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.fstore.core.io.DiskStorage.EOF;
import static io.joshworks.fstore.core.utils.Utils.sleep;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class StorageTest {

    protected static final int DEFAULT_LENGTH = 5242880;
    protected static final String TEST_DATA = "TEST-DATA";
    protected Storage storage;
    protected File testFile;

    protected abstract Storage store(File file, long size);

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        storage = store(testFile, DEFAULT_LENGTH);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        Utils.tryDelete(testFile);
    }

    @Test(expected = IllegalArgumentException.class)
    public void when_witting_empty_data_an_exception_is_thrown() {
        storage.write(ByteBuffer.allocate(0));
    }

    @Test
    public void when_data_is_written_return_the_written_length() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int written = storage.write(bb);
        assertEquals(TEST_DATA.length(), written);
    }

    @Test
    public void position_is_updated_on_insert() {
        int recordSize = 10;
        var buffer = ByteBuffer.allocate(recordSize);
        buffer.limit(buffer.capacity());

        assertEquals(0, storage.position());

        storage.write(buffer);
        assertEquals(recordSize, storage.position());
    }

    @Test
    public void when_data_is_read_it_must_be_the_same_that_was_written() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertTrue(Arrays.equals(bb.array(), result.array()));
    }

    @Test
    public void when_10000_entries_are_written_it_should_read_all_of_them() {
        int entrySize = 36; //uuid byte position

        int items = 10000;
        Set<String> inserted = new HashSet<>();
        for (int i = 0; i < items; i++) {
            String val = UUID.randomUUID().toString();
            inserted.add(val);
            storage.write(ByteBuffer.wrap(val.getBytes(StandardCharsets.UTF_8)));
        }

        long offset = 0;
        int itemsRead = 0;
        for (int i = 0; i < items; i++) {
            ByteBuffer bb = ByteBuffer.allocate(entrySize);
            int read = storage.read(offset, bb);
            assertEquals("Failed on iteration " + i, entrySize, read);

            bb.flip();
            String found = new String(bb.array(), StandardCharsets.UTF_8);
            assertTrue("Not found: [" + found + "] at offset " + offset + ", iteration: " + i, inserted.contains(found));
            itemsRead++;
            offset += entrySize;
        }

        assertEquals(items, itemsRead);
    }

    @Test
    public void when_writing_large_entry_it_should_read_the_same_value() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 10000; i++) {
            sb.append(UUID.randomUUID().toString());
        }

        String longString = sb.toString();

        ByteBuffer bb = ByteBuffer.wrap(longString.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertTrue(Arrays.equals(bb.array(), result.array()));
    }

    @Test
    public void delete() throws Exception {

        File temp = Utils.testFile();
        try (Storage store = store(temp, DEFAULT_LENGTH)) {
            store.delete();
            assertFalse(Files.exists(temp.toPath()));
        } finally {
            Utils.tryDelete(temp);
        }
    }

    @Test
    public void when_data_is_written_the_size_must_increase() {
        int dataLength = (int) storage.length();

        byte[] data = new byte[dataLength];
        Arrays.fill(data, (byte) 1);
        ByteBuffer bb = ByteBuffer.wrap(data);

        storage.write(bb);
        bb.clear();
        storage.write(bb);

        assertTrue(storage.length() >= dataLength);

        ByteBuffer found = ByteBuffer.allocate(dataLength);
        int read = storage.read(0, found);

        assertEquals(dataLength, read);
        assertTrue(Arrays.equals(data, found.array()));
    }

    @Test
    public void position_bigger_than_write_position_returns_EOF() {
        long writePosition = 4;
        storage.position(writePosition);
        ByteBuffer read = ByteBuffer.allocate(1024);
        int bytesRead = storage.read(writePosition + 1, read);
        assertEquals(EOF, bytesRead);
        read.flip();

        assertEquals(0, read.remaining());
    }

    @Test
    public void when_read_buffer_is_larger_than_available_data_only_available_is_returned() {
        int writeSize = 16;
        int readSize = 32;
        byte[] write = new byte[writeSize];
        Arrays.fill(write, (byte) 1);

        storage.write(ByteBuffer.wrap(write));

        ByteBuffer read = ByteBuffer.allocate(readSize);
        storage.read(0, read);
        read.flip();

        assertEquals(writeSize, read.remaining());
    }

    @Test
    public void position_is_updated() {
        long position = 10;
        storage.position(position);

        long found = storage.position();
        assertEquals(position, found);

        int value = 123;
        var data = ByteBuffer.allocate(Integer.BYTES).putInt(value).flip();
        storage.write(data);

        var read = ByteBuffer.allocate(Integer.BYTES);
        storage.read(position, read);

        assertEquals(value, read.flip().getInt());
    }

    @Test
    public void writing_returns_correct_written_bytes() {
        int entrySize = 255;
        byte[] data = new byte[entrySize];
        int totalItems = 1000000;
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        for (int i = 0; i < totalItems; i++) {
            int written = storage.write(ByteBuffer.wrap(data));
            assertEquals("Failed at position " + storage.position() + " iteration: " + i, entrySize, written);
        }

        assertEquals(totalItems * entrySize, storage.position());
    }

    @Test
    public void position_is_updated_same_as_writen_bytes() {
        int entrySize = 255;
        byte[] data = new byte[entrySize];
        int totalItems = 1000000;
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        long totalWriten = 0;
        for (int i = 0; i < totalItems; i++) {
            totalWriten += storage.write(ByteBuffer.wrap(data));
            assertEquals(totalWriten, storage.position());
        }

        assertEquals(totalItems * entrySize, storage.position());
    }

    @Test
    public void reading_return_the_same_writen_data() {
        int entrySize = 255;
        byte[] data = new byte[entrySize];
        int totalItems = 5000000;
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        for (int i = 0; i < totalItems; i++) {
            int written = storage.write(ByteBuffer.wrap(data));
            assertEquals("Failed at position " + storage.position() + " iteration: " + i, entrySize, written);
        }

        long readPos = 0;
        for (int i = 0; i < totalItems; i++) {
            var readBuffer = ByteBuffer.allocate(entrySize);
            int read = storage.read(readPos, readBuffer);
            assertEquals("Failed on pos " + readPos + " iteration " + i, entrySize, read);
            assertArrayEquals("Failed on pos " + readPos + " iteration " + i, data, readBuffer.array());
            readPos += read;
        }

        assertEquals(((long) totalItems * entrySize), storage.position());
    }

    @Test
    public void must_support_concurrent_reads_and_writes() throws InterruptedException {

        int items = 5000000;
        int entrySize = 255;

        byte[] data = new byte[entrySize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        final AtomicBoolean done = new AtomicBoolean();
        final AtomicBoolean writeFailed = new AtomicBoolean();
        final AtomicBoolean readFailed = new AtomicBoolean();

        Thread writer = new Thread(() -> {
            for (int i = 0; i < items; i++) {
                if (readFailed.get()) {
                    break;
                }
                try {
                    storage.write(ByteBuffer.wrap(data));
                } catch (Exception e) {
                    e.printStackTrace();
                    writeFailed.set(true);
                }
            }
            done.set(true);
        });


        Thread reader = new Thread(() -> {
            int readPos = 0;
            for (int i = 0; i < items; i++) {
                while (storage.position() <= readPos) {
                    sleep(2);
                }
                if (readFailed.get()) {
                    break;
                }
                try {
                    var readBuffer = ByteBuffer.allocate(entrySize);
                    readPos += storage.read(readPos, readBuffer);
                    readBuffer.flip();
                    if (!Arrays.equals(data, readBuffer.array())) {
                        System.err.println("POSITION: " + readPos);
                        System.err.println("EXPECTED: " + Arrays.toString(data));
                        System.err.println("FOUND   : " + Arrays.toString(readBuffer.array()));
                        readFailed.set(true);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    readFailed.set(true);
                }
            }
        });

        writer.start();
        reader.start();

        writer.join();
        reader.join();

        assertFalse(writeFailed.get());
        assertFalse(readFailed.get());
    }


    public static class RafStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size) {
            return StorageProvider.of(StorageMode.RAF).create(file, size);
        }

    }

    public static class RafCachedStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size) {
            return StorageProvider.of(StorageMode.RAF_CACHED).create(file, size);
        }

    }

    public static class MMapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size) {
            return StorageProvider.of(StorageMode.MMAP).create(file, size);
        }


        @Test
        public void buffer_grows_bigger_than_original_size() {
            long originalLength = storage.length();
            byte[] data = new byte[]{1};

            while (storage.position() < originalLength) {
                storage.write(ByteBuffer.wrap(data));
            }

            storage.write(ByteBuffer.wrap(data));

            assertTrue(storage.length() > originalLength);
        }
    }

}