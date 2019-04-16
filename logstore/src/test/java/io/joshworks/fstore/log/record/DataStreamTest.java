package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.GrowingThreadBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DataStreamTest {

    private static final int MAX_ENTRY_SIZE = 1024 * 1024 * 5;
    private static final double CHCKSUM_PROB = 1;

    private File file;
    private Storage storage;
    private static final long FILE_SIZE = Size.MB.of(10);

    private final IDataStream stream = new DataStream(new GrowingThreadBufferPool(false), CHCKSUM_PROB, MAX_ENTRY_SIZE);

    @Before
    public void setUp() {
        file = FileUtils.testFile();
        storage = StorageProvider.of(StorageMode.RAF).create(file, FILE_SIZE);
        storage.writePosition(Log.START);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(storage);
        FileUtils.tryDelete(file);
    }

    @Test
    public void write() {
        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long write1 = stream.write(storage, data1);

        assertEquals(Log.START, write1);
    }

    @Test
    public void reading_forward_returns_all_data() {

        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long pos1 = stream.write(storage, data1);

        RecordEntry<String> found = stream.read(storage, Direction.FORWARD, pos1, Serializers.STRING);
        assertNotNull(found);
        assertEquals("1", found.entry().intern());
    }

    @Test
    public void reading_backward_returns_all_data() {

        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        stream.write(storage, data1);

        long pos = storage.writePosition();
        RecordEntry<String> read = stream.read(storage, Direction.BACKWARD, pos, Serializers.STRING);
        assertNotNull(read);
        assertEquals("1", read.entry().intern());
    }

    @Test
    public void reading_forward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(storage, Serializers.INTEGER.toBytes(i));
        }

        List<RecordEntry<Integer>> entries = stream.bulkRead(storage, Direction.FORWARD, Log.START, Serializers.INTEGER);
        assertEquals(numItems, entries.size());

        for (int i = 0; i < numItems; i++) {
            assertEquals(Integer.valueOf(i), entries.get(i).entry());

        }
    }

    @Test
    public void readAllInto_backward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (long i = 0; i < numItems; i++) {
            stream.write(storage, Serializers.LONG.toBytes(i));
        }

        List<RecordEntry<Long>> found = stream.bulkRead(storage, Direction.BACKWARD, storage.writePosition(), Serializers.LONG);
        assertEquals(numItems, found.size());
        Collections.reverse(found);
        for (int i = 0; i < numItems; i++) {
            assertEquals(Long.valueOf(i), found.get(i).entry());

        }
    }

    @Test
    public void returned_size_matches_actual_entry_size() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(storage, Serializers.INTEGER.toBytes(i));
        }

        List<RecordEntry<Integer>> found = stream.bulkRead(storage, Direction.FORWARD, Log.START, Serializers.INTEGER);
        assertEquals((numItems * (Integer.BYTES + RecordHeader.HEADER_OVERHEAD)), found.stream().mapToInt(RecordEntry::recordSize).sum());
    }

    @Test
    public void read_forward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, Serializers.STRING.toBytes(first));

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, Serializers.STRING.toBytes(second));

        RecordEntry<String> found = stream.read(storage, Direction.FORWARD, Log.START, Serializers.STRING);
        assertNotNull(found);
        assertEquals(first, found.entry());
    }

    @Test
    public void read_backward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, Serializers.STRING.toBytes(first));

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, Serializers.STRING.toBytes(second));

        RecordEntry<String> read = stream.read(storage, Direction.BACKWARD, storage.writePosition(), Serializers.STRING);
        assertNotNull(read);
        assertEquals(second, read.entry());
    }

    @Test
    public void many_items() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        ByteBuffer data = Serializers.STRING.toBytes(content);
        long pos = stream.write(storage, data);

        RecordEntry<String> found = stream.read(storage, Direction.FORWARD, pos, Serializers.STRING);
        assertNotNull(found);
        assertEquals(content, found.entry().intern());
    }

    @Test
    public void reading_backward_with_data_bigger_than_page_returns_all_data() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        ByteBuffer data = Serializers.STRING.toBytes(content);
        stream.write(storage, data);

        long pos = storage.writePosition();
        RecordEntry<String> found = stream.read(storage, Direction.BACKWARD, pos, Serializers.STRING);
        assertNotNull(found);
        assertEquals(content, found.entry().intern());
    }

    @Test
    public void forward_bulkRead_additional_entry_is_ignored_when_bigger_than_remaining_bytes() {

        int bufferCapacity = DataStream.BULK_READ_BUFFER_SIZE;
        int firstEntryLength = bufferCapacity / 2; //second entry must have its header
        int secondEntryLength = bufferCapacity;

        String firstEntry = ofSize(firstEntryLength);
        stream.write(storage, Serializers.STRING.toBytes(firstEntry));
        String secondEntry = ofSize(secondEntryLength);
        stream.write(storage, Serializers.STRING.toBytes(secondEntry));

        List<RecordEntry<String>> read = stream.bulkRead(storage, Direction.FORWARD, Log.START, Serializers.STRING);
        assertEquals(1, read.size());
        assertEquals(firstEntry, read.get(0).entry().intern());
    }

    @Test
    public void backward_bulkRead_additional_entry_is_ignored_when_bigger_than_remaining_bytes() {

        int bufferCapacity = DataStream.BULK_READ_BUFFER_SIZE;
        int firstEntryLength = bufferCapacity / 2; //second entry must have its header
        int secondEntryLength = bufferCapacity;

        String firstEntry = ofSize(firstEntryLength);
        stream.write(storage, Serializers.STRING.toBytes(firstEntry));
        String secondEntry = ofSize(secondEntryLength);
        stream.write(storage, Serializers.STRING.toBytes(secondEntry));

        List<RecordEntry<String>> read = stream.bulkRead(storage, Direction.BACKWARD, storage.writePosition(), Serializers.STRING);
        assertEquals(1, read.size());
        assertEquals(secondEntry, read.get(0).entry().intern());
    }

    private static String ofSize(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append("a");
        }
        return sb.toString();
    }

}