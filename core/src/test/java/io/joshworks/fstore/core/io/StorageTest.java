package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.fstore.core.io.DiskStorage.EOF;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class StorageTest {

    private static final int STORAGE_SIZE = 5242880; //must not be greater than Integer.MAX_VALUE
    private static final int BUFFER_SIZE = STORAGE_SIZE;
    private static final String TEST_DATA = "TEST-DATA";
    private Storage storage;
    private File testFile;

    protected abstract Storage store(File file, long size, int bufferSize);

    @Before
    public void setUp() {
        testFile = Utils.testFile();
        storage = store(testFile, STORAGE_SIZE, BUFFER_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        Utils.tryDelete(testFile);
    }

    @Test(expected = StorageException.class)
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

        assertEquals(0, storage.writePosition());

        storage.write(buffer);
        assertEquals(recordSize, storage.writePosition());
    }

    @Test
    public void when_data_is_read_it_must_be_the_same_that_was_written() {
        ByteBuffer bb = ByteBuffer.wrap(TEST_DATA.getBytes(StandardCharsets.UTF_8));
        int write = storage.write(bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertArrayEquals(bb.array(), result.array());
    }

    @Test
    public void delete() throws Exception {

        File temp = Utils.testFile();
        try (Storage store = store(temp, STORAGE_SIZE, STORAGE_SIZE)) {
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
        storage.writePosition(writePosition);
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
        storage.writePosition(position);

        long found = storage.writePosition();
        assertEquals(position, found);

        int value = 123;
        var data = ByteBuffer.allocate(Integer.BYTES).putInt(value).flip();
        storage.write(data);

        var read = ByteBuffer.allocate(Integer.BYTES);
        storage.read(position, read);

        assertEquals(value, read.flip().getInt());
    }

    @Test
    public void EOF_is_returned_when_write_doesnt_fit_in_file() {
        int size = (int) storage.length();
        int written = storage.write(ByteBuffer.wrap(new byte[size]));
        assertEquals(size, written);

        written = storage.write(ByteBuffer.wrap(new byte[]{1}));
        assertEquals(EOF, written);
    }

    @Test
    public void writing_returns_correct_written_bytes() {
        byte[] bytes = fillWithUniqueBytes();
        int written = storage.write(ByteBuffer.wrap(bytes));
        assertEquals(bytes.length, written);
    }

    @Test
    public void position_is_updated_same_as_written_bytes() {
        byte[] data = fillWithUniqueBytes();
        int written;
        long total = 0;
        do {
            written = storage.write(ByteBuffer.wrap(data));
            total += written;
            if (written != EOF) {
                assertEquals(total, storage.writePosition());
            }
        } while (written > 0);
    }

    @Test
    public void data_can_be_written_up_to_storage_length() {
        int storeLen = (int) storage.length();
        byte[] data = new byte[storeLen];
        for (int i = 0; i < storeLen; i++) {
            data[i] = 0x1;
        }
        int written = storage.write(ByteBuffer.wrap(data));
        assertEquals(storeLen, written);

        long pos = storage.writePosition();
        assertEquals(storeLen, pos);

        var bb = ByteBuffer.allocate(storeLen);
        int read = storage.read(0, bb);
        bb.flip();
        assertEquals(storeLen, read);
        assertEquals(storeLen, bb.remaining());
    }

    @Test
    public void position_can_be_set_up_to_more_than_fileLength() {
        long storeLen = storage.length();
        storage.writePosition(storeLen);

        assertEquals(storeLen, storage.writePosition());
    }

    @Test(expected = StorageException.class)
    public void position_cannot_be_set_to_more_than_fileLength() {
        long storeLen = storage.length();
        storage.writePosition(storeLen + 1);
    }

    @Test
    public void when_position_is_equals_fileLength_then_trying_to_write_should_return_EOF() {
        int storeLen = (int) storage.length();
        byte[] data = new byte[storeLen];
        for (int i = 0; i < storeLen; i++) {
            data[i] = 0x1;
        }
        storage.write(ByteBuffer.wrap(data));
        int written = storage.write(ByteBuffer.wrap(new byte[]{1}));
        assertEquals(EOF, written);
    }

    @Test
    public void when_position_is_equals_fileLength_then_trying_to_read_should_return_EOF() {
        int storeLen = (int) storage.length();
        byte[] data = new byte[storeLen];
        for (int i = 0; i < storeLen; i++) {
            data[i] = 0x1;
        }
        storage.write(ByteBuffer.wrap(data));
        var bb = ByteBuffer.allocate(storeLen);
        int read = storage.read(storeLen, bb);
        assertEquals(EOF, read);
    }

    @Test
    public void when_position_is_equals_fileLength_then_trying_to_read_should_return_no_data() {
        int storeLen = (int) storage.length();
        byte[] data = new byte[storeLen];
        for (int i = 0; i < storeLen; i++) {
            data[i] = 0x1;
        }
        storage.write(ByteBuffer.wrap(data));
        var bb = ByteBuffer.allocate(storeLen);
        storage.read(storeLen, bb);
        bb.flip();
        assertEquals(0, bb.remaining());
    }

    @Test
    public void reading_return_the_same_written_data() {
        byte[] data = fillWithUniqueBytes();

        fillWith(storage, data);

        int read;
        long readPos = 0;
        do {
            var readBuffer = ByteBuffer.allocate(data.length);
            read = storage.read(readPos, readBuffer);
            readPos += read;
            if (read != EOF) {
                readBuffer.flip();
                assertArrayEquals("Failed on pos " + readPos, data, readBuffer.array());
            }
        } while (read != EOF);
    }

    @Test
    public void must_support_concurrent_reads_and_writes() throws InterruptedException {

        int items = 5000000;

        byte[] data = fillWithUniqueBytes();

        final AtomicBoolean done = new AtomicBoolean();
        final AtomicBoolean writeFailed = new AtomicBoolean();
        final AtomicBoolean readFailed = new AtomicBoolean();

        IOUtils.closeQuietly(storage);

        final var bigStorage = store(Utils.testFile(), Size.GB.of(1), Integer.MAX_VALUE - 8);
        try {
            Thread writer = new Thread(() -> {
                for (int i = 0; i < items; i++) {
                    try {
                        fillWith(bigStorage, data);
                    } catch (Exception e) {
                        e.printStackTrace();
                        writeFailed.set(true);
                        break;
                    }
                    if (!readFailed.get() && !writeFailed.get()) {
                        break;
                    }
                }
                done.set(true);
            });

            Thread reader = new Thread(() -> {
                int readPos = 0;
                int read;
                do {
                    try {
                        var readBuffer = ByteBuffer.allocate(data.length);
                        read = bigStorage.read(readPos, readBuffer);
                        readPos += read;
                        readBuffer.flip();
                        if (read != EOF && !Arrays.equals(data, readBuffer.array())) {
                            System.err.println("POSITION: " + readPos);
                            System.err.println("EXPECTED: " + Arrays.toString(data));
                            System.err.println("FOUND   : " + Arrays.toString(readBuffer.array()));
                            readFailed.set(true);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        readFailed.set(true);
                        break;
                    }
                } while (read != EOF && !readFailed.get() && !writeFailed.get());
            });

            writer.start();
            reader.start();

            writer.join();
            reader.join();

            assertFalse(writeFailed.get());
            assertFalse(readFailed.get());
        } finally {
            IOUtils.closeQuietly(bigStorage);
        }

    }

    @Test
    public void truncate() {
        byte[] data = fillWithUniqueBytes();

        storage.write(ByteBuffer.wrap(data));
        storage.write(ByteBuffer.wrap(data));

        long pos = storage.writePosition();
        storage.truncate();

        long afterPos = storage.writePosition();
        long length = storage.length();
        assertEquals(pos, afterPos);
        assertEquals(afterPos + 1, length);

        var bb = ByteBuffer.allocate(data.length);
        storage.read(0, bb);
        assertArrayEquals(data, bb.array());

        bb = ByteBuffer.allocate(data.length);
        storage.read(data.length, bb);
        assertArrayEquals(data, bb.array());

    }

    private byte[] fillWithUniqueBytes() {
        int entrySize = 255;
        byte[] data = new byte[entrySize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }
        return data;
    }

    private void fillWith(Storage storage, byte[] data) {
        int written;
        do {
            written = storage.write(ByteBuffer.wrap(data));
        } while (written > 0);
    }

    public void data_is_present_after_reopened_test() throws IOException {
        byte[] data = fillWithUniqueBytes();
        storage.write(ByteBuffer.wrap(data));
        storage.write(ByteBuffer.wrap(data));

        storage.close();
        storage = store(testFile, STORAGE_SIZE, BUFFER_SIZE);
        storage.writePosition(data.length * 2);

        var bb = ByteBuffer.allocate(data.length);
        storage.read(0, bb);
        assertArrayEquals(data, bb.array());

        bb = ByteBuffer.allocate(data.length);
        storage.read(data.length, bb);
        assertArrayEquals(data, bb.array());
    }

    public static class RafStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size, int bufferSize) {
            return new RafStorage(file, IOUtils.randomAccessFile(file, size));
        }

        @Test
        public void data_is_present_after_reopened() throws IOException {
            data_is_present_after_reopened_test();
        }
    }

    public static class RafCachedStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size, int bufferSize) {
            return new MMapCache(new RafStorage(file, IOUtils.randomAccessFile(file, size)), bufferSize);
        }

        @Test
        public void data_is_present_after_reopened() throws IOException {
            data_is_present_after_reopened_test();
        }
    }

    public static class MMapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size, int bufferSize) {
            return new MMapStorage(new RafStorage(file, IOUtils.randomAccessFile(file, size)), bufferSize);
        }

        @Test
        public void data_is_present_after_reopened() throws IOException {
            data_is_present_after_reopened_test();
        }
    }

    public static class OffHeapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size, int bufferSize) {
            return new OffHeapStorage(file.getName(), size, bufferSize);
        }
    }

    public static class HeapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size, int bufferSize) {
            return new HeapStorage(file.getName(), size, bufferSize);
        }
    }

}