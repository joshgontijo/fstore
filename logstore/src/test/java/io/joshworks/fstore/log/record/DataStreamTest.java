package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class DataStreamTest {

    private File file;
    private Storage storage;
    private static final long FILE_SIZE = Size.MB.of(10);

    private final IDataStream stream = new DataStream(new SingleBufferThreadCachedPool(false));

    @Before
    public void setUp() {
        file = FileUtils.testFile();
        storage = StorageProvider.of(StorageMode.RAF).create(file, FILE_SIZE);
        storage.position(Log.START);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(storage);
        FileUtils.tryDelete(file);
    }

    @Test
    public void write() {
        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long write1 = stream.write(storage,  data1);

        assertEquals(Log.START, write1);
    }

    @Test
    public void reading_forward_returns_all_data() {

        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long pos1 = stream.write(storage,  data1);

        try (BufferRef read = stream.read(storage,  Direction.FORWARD, pos1)) {
            String read1 = Serializers.STRING.fromBytes(read.get());
            assertEquals("1", read1.intern());
        }
    }

    @Test
    public void reading_backward_returns_all_data() {

        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        stream.write(storage,  data1);

        long pos = storage.position();
        try (BufferRef read = stream.read(storage,  Direction.BACKWARD, pos)) {
            String read1 = Serializers.STRING.fromBytes(read.get());
            assertEquals("1", read1.intern());
        }
    }

    @Test
    public void reading_forward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(storage,  Serializers.INTEGER.toBytes(i));
        }

        List<Integer> found = new ArrayList<>();
        try (BufferRef ref = stream.bulkRead(storage,  Direction.FORWARD, Log.START)) {
            ref.readAllInto(found, Serializers.INTEGER);
            assertEquals(numItems, found.size());

            for (int i = 0; i < numItems; i++) {
                assertEquals(Integer.valueOf(i), found.get(i));

            }
        }
    }

    @Test
    public void readAllInto_backward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (long i = 0; i < numItems; i++) {
            stream.write(storage,  Serializers.LONG.toBytes(i));
        }

        List<Long> found = new ArrayList<>();
        try (BufferRef ref = stream.bulkRead(storage,  Direction.BACKWARD, storage.position())) {
            ref.readAllInto(found, Serializers.LONG);
            assertEquals(numItems, found.size());
            Collections.reverse(found);

            for (int i = 0; i < numItems; i++) {
                assertEquals(Long.valueOf(i), found.get(i));

            }
        }
    }

    @Test
    public void reading_allInto_size() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(storage,  Serializers.INTEGER.toBytes(i));
        }

        List<Integer> found = new ArrayList<>();
        try (BufferRef ref = stream.bulkRead(storage,  Direction.FORWARD, Log.START)) {
            int[] read = ref.readAllInto(found, Serializers.INTEGER);
            assertEquals((numItems * (Integer.BYTES + RecordHeader.HEADER_OVERHEAD)), IntStream.of(read).sum());
        }
    }

    @Test
    public void read_forward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage,  Serializers.STRING.toBytes(first));

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage,  Serializers.STRING.toBytes(second));

        List<String> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage,  Direction.FORWARD, Log.START)) {
            ref.readAllInto(found, Serializers.STRING);
            assertEquals(1, found.size());
            assertEquals(first, found.get(0));
        }
    }

    @Test
    public void read_backward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage,  Serializers.STRING.toBytes(first));

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage,  Serializers.STRING.toBytes(second));

        List<String> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage,  Direction.BACKWARD, storage.position())) {
            ref.readAllInto(found, Serializers.STRING);
            assertEquals(1, found.size());
            assertEquals(second, found.get(0));
        }
    }

    @Test
    public void many_items() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        ByteBuffer data = Serializers.STRING.toBytes(content);
        long pos = stream.write(storage,  data);

        try (BufferRef read = stream.read(storage,  Direction.FORWARD, pos)) {
            String readContent = Serializers.STRING.fromBytes(read.get());
            assertEquals(content, readContent.intern());
        }
    }

    @Test
    public void reading_backward_with_data_bigger_than_page_returns_all_data() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        ByteBuffer data = Serializers.STRING.toBytes(content);
        stream.write(storage,  data);

        long pos = storage.position();
        try (BufferRef read = stream.read(storage,  Direction.BACKWARD, pos)) {
            String read1 = Serializers.STRING.fromBytes(read.get());
            assertEquals(content, read1.intern());
        }
    }

    private static String ofSize(int size) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < size; i++) {
            sb.append("a");
        }
        return sb.toString();
    }

}