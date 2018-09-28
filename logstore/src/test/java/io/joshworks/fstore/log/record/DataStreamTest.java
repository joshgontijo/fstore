package io.joshworks.fstore.log.record;

import io.joshworks.fstore.core.io.BufferPool;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Utils;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class DataStreamTest {

    private File file;
    private Storage storage;

    private final IDataStream stream = new DataStream();
    private final BufferPool pool = new FixedPageSizeBuffer();

    @Before
    public void setUp() {
        file = Utils.testFile();
        storage = new RafStorage(file, 1024 * 1024, Mode.READ_WRITE);
        storage.position(Log.START); //Reader doesn't read entries less than Log.START
    }

    @Test
    public void write() {
        ByteBuffer data1 = Serializers.STRING.toBytes("1");
        long write1 = stream.write(storage, pool, data1);

        assertEquals(Log.START, write1);
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