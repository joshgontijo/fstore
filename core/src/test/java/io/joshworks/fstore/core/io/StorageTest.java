package io.joshworks.fstore.core.io;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

import static io.joshworks.fstore.core.io.DiskStorage.EOF;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class StorageTest {

    private static final int STORAGE_SIZE = Size.MB.ofInt(100);
    private Storage storage;
    private File testFile;

    protected abstract Storage store(File file, long size);

    @Before
    public void setUp() {
        testFile = TestUtils.testFile();
        testFile.deleteOnExit();
        storage = store(testFile, STORAGE_SIZE);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(storage);
        TestUtils.deleteRecursively(testFile);
    }

    @Test(expected = StorageException.class)
    public void when_witting_empty_data_an_exception_is_thrown() {
        storage.write(ByteBuffer.allocate(0));
    }

    @Test
    public void when_data_is_written_return_the_written_length() {
        String testData = "TEST-DATA";
        ByteBuffer bb = ByteBuffer.wrap(testData.getBytes(StandardCharsets.UTF_8));
        int written = storage.write(bb);
        assertEquals(testData.length(), written);
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
        ByteBuffer bb = ByteBuffer.wrap("TEST-DATA".getBytes(StandardCharsets.UTF_8));
        int write = storage.write(bb);

        ByteBuffer result = ByteBuffer.allocate(bb.capacity());
        int read = storage.read(0, result);

        assertEquals(write, read);
        assertArrayEquals(bb.array(), result.array());
    }

    @Test
    public void delete() throws Exception {

        File temp = TestUtils.testFile();
        try (Storage store = store(temp, STORAGE_SIZE)) {
            store.delete();
            assertFalse(Files.exists(temp.toPath()));
        } finally {
            TestUtils.deleteRecursively(temp);
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
        assertArrayEquals(data, found.array());
    }

    @Test
    public void read_position_ahead_write_position_returns_EOF() {
        long position = 4;
        storage.position(position);
        ByteBuffer read = ByteBuffer.allocate(1024);
        int bytesRead = storage.read(position + 1, read);
        assertEquals(EOF, bytesRead);
        read.flip();

        assertEquals(0, read.remaining());
    }

    @Test
    public void when_position_reaches_the_end_of_the_file_it_expands_the_file() {
        long initialLength = storage.length();
        long writePos = initialLength - 1;
        byte[] data = fillWithUniqueBytes();

        storage.position(writePos);
        storage.write(ByteBuffer.wrap(data));

        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        storage.read(writePos, readBuffer);

        assertTrue(storage.length() > initialLength);
    }

    @Test
    public void data_is_stored_write_causes_store_to_be_expanded() {
        long initialLength = storage.length();
        long writePos = initialLength - 1;
        byte[] data = fillWithUniqueBytes();

        storage.position(writePos);
        storage.write(ByteBuffer.wrap(data));

        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        int read = storage.read(writePos, readBuffer);

        assertEquals(data.length, read);
        assertArrayEquals(data, readBuffer.array());
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
        byte[] bytes = fillWithUniqueBytes();
        int written = storage.write(ByteBuffer.wrap(bytes));
        assertEquals(bytes.length, written);
    }

    @Test
    public void position_is_updated_same_as_written_bytes() {
        byte[] data = fillWithUniqueBytes();
        int items = (int) (storage.length() / data.length);

        int written;
        long total = 0;
        for (int i = 0; i < items; i++) {
            written = storage.write(ByteBuffer.wrap(data));
            total += written;
            assertEquals(total, storage.position());
        }
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

        long pos = storage.position();
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
        storage.position(storeLen);

        assertEquals(storeLen, storage.position());
    }

    @Test
    public void when_position_is_equal_or_great_than_fileLength_then_trying_to_read_should_return_EOF() {
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
    public void EOF_is_returned_when_position_is_set_to_equals_or_greater_than_fileLength() {
        long pos = storage.length() + 1;

        storage.position(pos);
        assertEquals(EOF, storage.read(pos, ByteBuffer.allocate(1)));
    }

    @Test
    public void file_expands_to_position_after_setting_position_greater_than_file_length_and_writing_data_afterwards() {
        byte[] data = new byte[]{65};
        long pos = storage.length() + 1;
        storage.position(pos);
        int written = storage.write(ByteBuffer.wrap(data));
        assertEquals(data.length, written);

        //entry length must also be taken into account
        assertEquals(pos + data.length, storage.length());
    }

    //this mainly affects MemStorage
    @Test
    public void MEM_SPECIFIC___buffer_expands_as_many_buffers_are_required() {
        byte[] data = new byte[]{65};
        long pos = storage.length() + Integer.MAX_VALUE + 1;
        storage.position(pos);
        int written = storage.write(ByteBuffer.wrap(data));
        assertEquals(data.length, written);

        //entry length must also be taken into account
        assertEquals(pos + data.length, storage.length());
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
            if (read > 0) {
                readBuffer.flip();
                assertArrayEquals("Failed on pos " + readPos, data, readBuffer.array());
            }
            readPos += read;
        } while (read != EOF);
    }

    @Test
    public void truncate_does_not_change_file_when_provided_size_is_greater_than_current() {
        long size = storage.length();
        storage.truncate(size + 1);
        assertEquals(size, storage.length());
    }

    @Test
    public void truncate_reduces_file_size_to_specified_size() {
        long size = storage.length();
        long newSize = size - 1;
        storage.truncate(newSize);

        long found = storage.length();
        assertEquals(newSize, found);
    }


    @Test
    public void truncate_reduces_sets_position_to_the_end_of_file_if_position_is_greater_than_provided_new_length() {
        long size = storage.length();
        storage.position(size);
        long newSize = size - 1;
        storage.truncate(newSize);

        long afterTruncation = storage.position();
        assertEquals(newSize, afterTruncation);
    }

    @Test
    public void truncate_does_not_change_position_if_position_is_less_than_than_provided_new_length() {
        long size = storage.length();
        long newSize = size - 1;
        long position = 1;
        storage.position(position);
        storage.truncate(newSize);

        long afterTruncation = storage.position();
        assertEquals(position, afterTruncation);
    }

    @Test
    public void position_can_set_to_higher_position_after_expanding() {
        byte[] data = fillWithUniqueBytes();

        fillWith(storage, data);
        long writePos = storage.position();
        long length = storage.length();

        int additionalSize = (int) (length - writePos + 1);
        storage.write(ByteBuffer.allocate(additionalSize));

        storage.position(0);
        storage.position(storage.length());

        assertEquals(storage.length(), storage.position());
    }

    @Test
    public void when_position_is_provided_to_write_then_internal_position_should_not_change() {
        long position = storage.position();

        byte[] data = fillWithUniqueBytes();
        storage.write(10, ByteBuffer.wrap(data));
        long afterWrite = storage.position();
        assertEquals(position, afterWrite);
    }

    @Test
    public void gathering_returns_written_total_of_all_buffers() {
        ByteBuffer first = ByteBuffer.wrap(fillWithUniqueBytes());
        ByteBuffer second = ByteBuffer.wrap(fillWithUniqueBytes());

        long totalSize = first.capacity() + second.capacity();
        long written = storage.write(new ByteBuffer[]{first, second});

        assertEquals(totalSize, written);
    }

    @Test
    public void gathering_stores_all_data_in_right_order() {
        ByteBuffer first = ByteBuffer.wrap(fillWithUniqueBytes());
        ByteBuffer second = ByteBuffer.wrap(fillWithUniqueBytes());

        int totalSize = first.capacity() + second.capacity();
        storage.write(new ByteBuffer[]{first, second});

        var readBuffer = ByteBuffer.allocate(totalSize);

        int read = storage.read(0, readBuffer);
        assertEquals(totalSize, read);

        readBuffer.flip();
        assertEquals(totalSize, readBuffer.remaining());

        readBuffer.clear().limit(first.limit());
        assertEquals(first.clear(), readBuffer.slice());

        readBuffer.clear().limit(readBuffer.capacity()).position(first.capacity());
        assertEquals(first.clear(), readBuffer.slice());
    }

    @Test
    public void gathering_write_expands_store_to_accommodate_data() {
        ByteBuffer first = ByteBuffer.wrap(fillWithUniqueBytes());
        ByteBuffer second = ByteBuffer.wrap(fillWithUniqueBytes());

        int totalSize = first.capacity() + second.capacity();
        storage.write(new ByteBuffer[]{first, second});

        var readBuffer = ByteBuffer.allocate(totalSize);

        int read = storage.read(0, readBuffer);
        assertEquals(totalSize, read);

        readBuffer.flip();
        assertEquals(totalSize, readBuffer.remaining());

        readBuffer.clear().limit(first.limit());
        assertEquals(first.clear(), readBuffer.slice());

        readBuffer.clear().limit(readBuffer.capacity()).position(first.capacity());
        assertEquals(first.clear(), readBuffer.slice());
    }

    @Test
    public void can_random_read_position_less_than_current() {
        byte[] data = {1, 1};

        int written = storage.write(ByteBuffer.wrap(data));
        assertEquals(data.length, written);

        storage.position(10);

        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        int read = storage.read(0, readBuffer);
        assertEquals(data.length, read);
        assertArrayEquals(data, readBuffer.array());
    }

    @Test
    public void random_read_of_position_greater_than_current_position_returns_EOF() {
        byte[] data = {1, 1};

        storage.position(10);
        int written = storage.write(ByteBuffer.wrap(data));
        assertEquals(data.length, written);

        storage.position(0);

        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        int read = storage.read(10, readBuffer);
        assertEquals(EOF, read);
    }

    @Test
    public void can_random_write_to_position_greater_than_current() {
        byte[] data = {1, 1};

        long randWritePos = 200;

        int written = storage.write(randWritePos, ByteBuffer.wrap(data));
        assertEquals(data.length, written);

        storage.position(randWritePos + written);

        ByteBuffer readBuffer = ByteBuffer.allocate(data.length);
        int read = storage.read(randWritePos, readBuffer);
        assertEquals(data.length, read);
        assertArrayEquals(data, readBuffer.array());
    }

    @Test
    public void file_length_is_increased_when_random_write_to_position_greater_than_file_length() {
        byte[] data = {1, 1};
        long originalLen = storage.length();

        int written = storage.write(originalLen + 10, ByteBuffer.wrap(data));
        assertEquals(data.length, written);

        assertTrue(storage.length() > originalLen);
    }

    @Test
    public void random_write_does_not_update_internal_position() {
        storage.write(0, ByteBuffer.wrap(fillWithUniqueBytes()));
        assertEquals(0, storage.position());
    }

    @Test
    public void absolute_write_does_not_advance_storage_position() {
        byte[] data = fillWithUniqueBytes();

        long pos = storage.position();
        int written = storage.write(0, ByteBuffer.wrap(data));
        assertEquals(data.length, written);
        assertEquals(pos, storage.position());
    }

    @Test
    public void gather_write_advances_storage_position() {
        ByteBuffer data1 = ByteBuffer.wrap(fillWithUniqueBytes());
        ByteBuffer data2 = ByteBuffer.wrap(fillWithUniqueBytes());

        long pos = storage.position();
        long written = storage.write(new ByteBuffer[]{data1, data2});
        assertEquals(pos + written, storage.position());
    }

    @Test
    public void gather_write_expand_file_when_no_total_of_remaining_items_is_greater_than_store_space() {
        long originalLen = storage.length();

        byte[] data = fillWithUniqueBytes();
        fillWith(storage, data);

        storage.write(new ByteBuffer[]{ByteBuffer.allocate(data.length * 5), ByteBuffer.allocate(data.length * 5)});
        assertTrue(originalLen < storage.length());
    }

    @Test
    public void absolute_write_expand_file_when_no_total_of_remaining_items_is_greater_than_store_space() {

        long originalLen = storage.length();

        byte[] data = fillWithUniqueBytes();
        fillWith(storage, data);

        storage.write(storage.position(), ByteBuffer.allocate(data.length * 5));
        assertTrue(originalLen < storage.length());
    }

    @Test
    public void relative_write_expand_file_when_no_total_of_remaining_items_is_greater_than_store_space() {
        long originalLen = storage.length();

        byte[] data = fillWithUniqueBytes();
        fillWith(storage, data);

        storage.write(ByteBuffer.allocate(data.length * 5));
        assertTrue(originalLen < storage.length());
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
        int items = (int) (storage.length() / data.length);
        for (int i = 0; i < items; i++) {
            storage.write(ByteBuffer.wrap(data));
        }
    }

    void data_is_present_after_reopened_test() throws IOException {
        byte[] data = fillWithUniqueBytes();
        storage.write(ByteBuffer.wrap(data));
        storage.write(ByteBuffer.wrap(data));

        storage.close();
        storage = store(testFile, STORAGE_SIZE);
        storage.position(data.length * 2);

        var bb = ByteBuffer.allocate(data.length);
        storage.read(0, bb);
        assertArrayEquals(data, bb.array());

        bb = ByteBuffer.allocate(data.length);
        storage.read(data.length, bb);
        assertArrayEquals(data, bb.array());
    }

    @Test
    public void MEM_SPECIFIC_relative_write_updates_internal_buffer_position() {
        byte[] data = fillWithUniqueBytes();
        byte[] garbage = new byte[]{111, 111};

        storage.write(ByteBuffer.wrap(data));
        storage.write(new ByteBuffer[]{ByteBuffer.wrap(garbage), ByteBuffer.wrap(garbage)});

        ByteBuffer readData = ByteBuffer.allocate(data.length);
        int read = storage.read(0, readData);
        assertEquals(data.length, read);
        assertArrayEquals(data, readData.array());
    }


    public static class DiskStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long length) {
            return new DiskStorage(file, length, IOUtils.randomAccessFile(file, length));
        }

        @Test
        public void data_is_present_after_reopened() throws IOException {
            data_is_present_after_reopened_test();
        }
    }

    public static class MMapCacheTest extends StorageTest {

        @Override
        protected Storage store(File file, long length) {
            return new MMapCache(new DiskStorage(file, length, IOUtils.randomAccessFile(file, length)));
        }

        @Test
        public void data_is_present_after_reopened() throws IOException {
            data_is_present_after_reopened_test();
        }
    }

    public static class MMapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long length) {
            return new MMapStorage(new DiskStorage(file, length, IOUtils.randomAccessFile(file, length)));
        }

        @Test
        public void data_is_present_after_reopened() throws IOException {
            data_is_present_after_reopened_test();
        }
    }

    public static class OffHeapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size) {
            return new OffHeapStorage(file.getName(), size);
        }
    }

    public static class HeapStorageTest extends StorageTest {

        @Override
        protected Storage store(File file, long size) {
            return new HeapStorage(file.getName(), size);
        }
    }

}