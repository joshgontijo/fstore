package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.Utils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class DataStreamTest {

    private File file;
    private Storage storage;

    private static final int START = 0;

    private final IDataStream stream = new DataStream(START);
    private final BufferPool pool = new FixedPageSizeBuffer();

    @Before
    public void setUp() {
        file = Utils.testFile();
        storage = new RafStorage(file, 1024 * 1024, Mode.READ_WRITE);
    }

    @Test
    public void write() {
        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long write1 = stream.write(storage, pool, data1);

        assertEquals(START, write1);
    }

    @Test
    public void reading_forward_returns_all_data() {

        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long pos1 = stream.write(storage, pool, data1);

        try (BufferRef read = stream.read(storage, pool, Direction.FORWARD, pos1)) {
            String read1 = Serializers.STRING.fromBytes(read.get());
            assertEquals("1", read1.intern());
        }
    }

    @Test
    public void reading_backward_returns_all_data() {

        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        stream.write(storage, pool, data1);

        long pos = storage.position();
        try (BufferRef read = stream.read(storage, pool, Direction.BACKWARD, pos)) {
            String read1 = Serializers.STRING.fromBytes(read.get());
            assertEquals("1", read1.intern());
        }
    }

    @Test
    public void reading_forward_with_data_bigger_than_page_returns_all_data() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(storage, pool, Serializers.INTEGER.toBytes(i));
        }

        List<Integer> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage, pool, Direction.FORWARD, START)) {
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
            stream.write(storage, pool, Serializers.LONG.toBytes(i));
        }

        List<Long> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage, pool, Direction.BACKWARD, storage.position())) {
            ref.readAllInto(found, Serializers.LONG);
            assertEquals(numItems, found.size());

            for (int i = 0; i > numItems; i++) {
                assertEquals(Long.valueOf(i), found.get(i));

            }
        }
    }

    @Test
    public void reading_allInto_size() {

        int numItems = 10;
        for (int i = 0; i < numItems; i++) {
            stream.write(storage, pool, Serializers.INTEGER.toBytes(i));
        }

        List<Integer> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage, pool, Direction.FORWARD, START)) {
            int[] read = ref.readAllInto(found, Serializers.INTEGER);
            assertEquals((numItems * (Integer.BYTES + RecordHeader.HEADER_OVERHEAD)), read);
        }
    }

    @Test
    public void read_forward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, pool, Serializers.STRING.toBytes(first));

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, pool, Serializers.STRING.toBytes(second));

        List<String> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage, pool, Direction.FORWARD, START)) {
            ref.readAllInto(found, Serializers.STRING);
            assertEquals(1, found.size());
            assertEquals(first, found.get(0));
        }
    }

    @Test
    public void read_backward_returns_only_whole_entry_data() {

        String first = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, pool, Serializers.STRING.toBytes(first));

        String second = ofSize(Memory.PAGE_SIZE / 2);
        stream.write(storage, pool, Serializers.STRING.toBytes(second));

        List<String> found = new ArrayList<>();
        try (BufferRef ref = stream.read(storage, pool, Direction.BACKWARD, storage.position())) {
            ref.readAllInto(found, Serializers.STRING);
            assertEquals(1, found.size());
            assertEquals(second, found.get(0));
        }
    }

    @Test
    public void many_items() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        ByteBuffer data = Serializers.STRING.toBytes(content);
        long pos = stream.write(storage, pool, data);

        try (BufferRef read = stream.read(storage, pool, Direction.FORWARD, pos)) {
            String readContent = Serializers.STRING.fromBytes(read.get());
            assertEquals(content, readContent.intern());
        }
    }

    @Test
    public void reading_backward_with_data_bigger_than_page_returns_all_data() {

        String content = ofSize(Memory.PAGE_SIZE + 1);
        ByteBuffer data = Serializers.STRING.toBytes(content);
        stream.write(storage, pool, data);

        long pos = storage.position();
        try (BufferRef read = stream.read(storage, pool, Direction.BACKWARD, pos)) {
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