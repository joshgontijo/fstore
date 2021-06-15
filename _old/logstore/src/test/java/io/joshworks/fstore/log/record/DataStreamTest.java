package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DataStreamTest {

    private static final double CHCKSUM_PROB = 1;

    private File file;
    private Storage storage;
    private BufferPool bufferPool;
    private static final long FILE_SIZE = Size.MB.of(10);
    private static final int MAX_ENTRY_SIZE = Size.MB.ofInt(1);

    private DataStream stream;

    @Before
    public void setUp() {
        file = TestUtils.testFile();
        storage = Storage.create(file, StorageMode.RAF, FILE_SIZE);
        storage.position(Log.START);
        bufferPool = new ThreadLocalBufferPool(MAX_ENTRY_SIZE, false);
        stream = new DataStream(bufferPool, storage, CHCKSUM_PROB, Memory.PAGE_SIZE);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(storage);
        TestUtils.deleteRecursively(file);
    }

    @Test
    public void write() {
        long write1 = stream.write("1", Serializers.STRING);

        assertEquals(Log.START, write1);
    }

    @Test
    public void write_typed() {
        String data = "abcdef";
        long pos = stream.write(data, Serializers.VSTRING);
        RecordEntry<String> record = stream.read(Direction.FORWARD, pos, Serializers.VSTRING);
        assertEquals(data, record.entry());
    }

    @Test(expected = IllegalArgumentException.class)
    public void maximum_entry_size_is_maxEntrySize_MINUS_RECORDHEADER_HEADER_OVERHEAD() {
        int capacity = bufferPool.bufferSize();
        byte[] data = new byte[capacity - RecordHeader.HEADER_OVERHEAD + 1];
        Arrays.fill(data, (byte) 65);
        String longString = new String(data);

        stream.write(longString, Serializers.VSTRING);
    }

    @Test
    public void writing_relative_position_less_than_store_position() {
        Integer entry = 12345;
        storage.position(10);
        stream.write(0, entry, Serializers.INTEGER);
        RecordEntry<Integer> record = stream.read(Direction.FORWARD, 0, Serializers.INTEGER);
        assertEquals(entry, record.entry());
    }

    @Test
    public void writing_relative_position_greater_than_store_position() {
        Integer entry = 12345;
        long writePos = 20;
        storage.position(10);
        stream.write(writePos, entry, Serializers.INTEGER);
        storage.position(storage.length());
        RecordEntry<Integer> record = stream.read(Direction.FORWARD, writePos, Serializers.INTEGER);
        assertEquals(entry, record.entry());
    }

    @Test
    public void writing_relative_position_greater_than_store_position_does_not_change_store_position() {
        Integer entry = 12345;
        storage.position(10);
        stream.write(20, entry, Serializers.INTEGER);
        assertEquals(10, storage.position());
    }

    @Test
    public void writing_relative_position_less_than_store_position_does_not_change_store_position() {
        Integer entry = 12345;
        storage.position(10);
        stream.write(5, entry, Serializers.INTEGER);
        assertEquals(10, storage.position());
    }

    @Test(expected = IllegalArgumentException.class)
    public void write_typed_throws_error_if_secondary_header_cannot_be_written() {
        int capacity = bufferPool.bufferSize();
        byte[] data = new byte[capacity - RecordHeader.HEADER_OVERHEAD + 1];
        Arrays.fill(data, (byte) 65);
        String longString = new String(data);

        stream.write(longString, Serializers.STRING);
    }

    @Test
    public void reading_forward_returns_all_data() {

        long pos1 = stream.write("1", Serializers.STRING);

        RecordEntry<String> found = stream.read(Direction.FORWARD, pos1, Serializers.STRING);
        assertNotNull(found);
        assertEquals("1", found.entry().intern());
    }

    @Test
    public void reading_backward_returns_all_data() {

        stream.write("1", Serializers.STRING);

        long pos = storage.position();
        RecordEntry<String> read = stream.read(Direction.BACKWARD, pos, Serializers.STRING);
        assertNotNull(read);
        assertEquals("1", read.entry().intern());
    }

    @Test
    public void reading_forward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(i, Serializers.INTEGER);
        }

        List<RecordEntry<Integer>> entries = stream.bulkRead(Direction.FORWARD, Log.START, Serializers.INTEGER);
        assertEquals(numItems, entries.size());

        for (int i = 0; i < numItems; i++) {
            assertEquals(Integer.valueOf(i), entries.get(i).entry());

        }
    }

    @Test
    public void readAllInto_backward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (long i = 0; i < numItems; i++) {
            stream.write(i, Serializers.LONG);
        }

        List<RecordEntry<Long>> found = stream.bulkRead(Direction.BACKWARD, storage.position(), Serializers.LONG);
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
            stream.write(i, Serializers.INTEGER);
        }

        List<RecordEntry<Integer>> found = stream.bulkRead(Direction.FORWARD, Log.START, Serializers.INTEGER);
        assertEquals((numItems * (Integer.BYTES + RecordHeader.HEADER_OVERHEAD)), found.stream().mapToInt(RecordEntry::recordSize).sum());
    }

    @Test
    public void read_forward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(first, Serializers.STRING);

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(second, Serializers.STRING);

        RecordEntry<String> found = stream.read(Direction.FORWARD, Log.START, Serializers.STRING);
        assertNotNull(found);
        assertEquals(first, found.entry());
    }

    @Test
    public void read_backward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(first, Serializers.STRING);

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(second, Serializers.STRING);

        RecordEntry<String> read = stream.read(Direction.BACKWARD, storage.position(), Serializers.STRING);
        assertNotNull(read);
        assertEquals(second, read.entry());
    }

    @Test
    public void many_items() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        long pos = stream.write(content, Serializers.STRING);

        RecordEntry<String> found = stream.read(Direction.FORWARD, pos, Serializers.STRING);
        assertNotNull(found);
        assertEquals(content, found.entry().intern());
    }

    @Test
    public void reading_backward_with_data_bigger_than_page_returns_all_data() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        stream.write(content, Serializers.STRING);

        long pos = storage.position();
        RecordEntry<String> found = stream.read(Direction.BACKWARD, pos, Serializers.STRING);
        assertNotNull(found);
        assertEquals(content, found.entry().intern());
    }

    @Test
    public void forward_bulkRead_additional_entry_is_ignored_when_bigger_than_remaining_bytes() {

        int bufferCapacity = Memory.PAGE_SIZE;
        int firstEntryLength = bufferCapacity / 2; //second entry must have its header
        int secondEntryLength = bufferCapacity;

        String firstEntry = ofSize(firstEntryLength);
        stream.write(firstEntry, Serializers.STRING);
        String secondEntry = ofSize(secondEntryLength);
        stream.write(secondEntry, Serializers.STRING);

        List<RecordEntry<String>> read = stream.bulkRead(Direction.FORWARD, Log.START, Serializers.STRING);
        assertEquals(1, read.size());
        assertEquals(firstEntry, read.get(0).entry().intern());
    }

    @Test
    public void backward_bulkRead_additional_entry_is_ignored_when_bigger_than_remaining_bytes() {

        int bufferCapacity = Memory.PAGE_SIZE;
        int firstEntryLength = bufferCapacity / 2; //second entry must have its header
        int secondEntryLength = bufferCapacity;

        String firstEntry = ofSize(firstEntryLength);
        stream.write(firstEntry, Serializers.STRING);
        String secondEntry = ofSize(secondEntryLength);
        stream.write(secondEntry, Serializers.STRING);

        List<RecordEntry<String>> read = stream.bulkRead(Direction.BACKWARD, storage.position(), Serializers.STRING);
        assertEquals(1, read.size());
        assertEquals(secondEntry, read.get(0).entry().intern());
    }

    @Test
    public void readForward_returns_RecordEntry_with_correct_position() {
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            long pos = stream.write(String.valueOf(i), Serializers.STRING);
            positions.add(pos);
        }

        for (Long position : positions) {
            RecordEntry<String> record = stream.read(Direction.FORWARD, position, Serializers.STRING);
            assertEquals((long) position, record.position());
        }
    }

    @Test
    public void readBackward_returns_RecordEntry_with_correct_position() {
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            stream.write(String.valueOf(i), Serializers.STRING);
            positions.add(stream.position());
        }

        Collections.reverse(positions);
        for (Long position : positions) {
            RecordEntry<String> record = stream.read(Direction.BACKWARD, position, Serializers.STRING);
            assertEquals((long) position, record.position());
        }
    }

    @Test
    public void readBulkForward_returns_RecordEntry_with_correct_position() {
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            long pos = stream.write(String.valueOf(i), Serializers.STRING);
            positions.add(pos);
        }

        Set<Long> positionSet = new HashSet<>(positions);
        for (Long position : positions) {
            List<RecordEntry<String>> entries = stream.bulkRead(Direction.FORWARD, position, Serializers.STRING);
            for (RecordEntry<String> entry : entries) {
                assertTrue(positionSet.contains(entry.position()));
            }
        }
    }

    @Test
    public void readBulkBackward_returns_RecordEntry_with_correct_position() {
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            long pos = stream.write(String.valueOf(i), Serializers.STRING);
            positions.add(pos);
        }

        Set<Long> positionSet = new HashSet<>(positions);
        for (Long position : positions) {
            List<RecordEntry<String>> entries = stream.bulkRead(Direction.BACKWARD, position, Serializers.STRING);
            for (RecordEntry<String> entry : entries) {
                assertTrue(positionSet.contains(entry.position()));
            }
        }
    }

    @Test
    public void when_serializer_set_the_dst_limit_than_dataStream_should_extend_its_capacity_to_accomodate_secondary_header() {
        Integer entry = 12345;
        storage.position(10);
        stream.write(5, entry, Serializers.INTEGER);
        assertEquals(10, storage.position());
    }

    private static String ofSize(int size) {
        StringBuilder sb = new StringBuilder();
        sb.append("a".repeat(Math.max(0, size)));
        return sb.toString();
    }

    private static class SetDstLimitSerializer implements Serializer<Integer> {

        @Override
        public void writeTo(Integer data, ByteBuffer dst) {

        }

        @Override
        public Integer fromBytes(ByteBuffer buffer) {
            return null;
        }
    }

}