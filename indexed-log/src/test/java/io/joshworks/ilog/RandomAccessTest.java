package io.joshworks.ilog;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RandomAccessTest {

    private IndexedSegment<Integer> segment;
    private ByteBuffer writeBuffer = Buffers.allocate(4096, false);

    private static int ITEMS = 25000000;

    @Before
    public void setUp() throws IOException {
        segment = new IndexedSegment<>(TestUtils.testFile(), Size.GB.ofInt(1), KeyParser.INT);
    }

    @After
    public void tearDown() throws IOException {
        segment.delete();
    }

    @Test
    public void write() throws IOException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            segment.append(create(i, "value-" + i));
            writeBuffer.clear();
        }
        System.out.println("WRITE: " + ITEMS + " IN " + (System.currentTimeMillis() - start));

        long s = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            Record rec = segment.read(i);
//            System.out.println(rec);
        }
        System.out.println("READ: " + ITEMS + " IN " + (System.currentTimeMillis() - s));
    }

    @Test
    public void write2() {
        Segment<String> seg = new Segment<>(TestUtils.testFile(), StorageMode.RAF, Size.GB.of(1), Serializers.VSTRING, new SimpleBufferPool("a", 4096, false), WriteMode.LOG_HEAD, 1);
        long start = System.currentTimeMillis();
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < ITEMS; i++) {
            long pos = seg.append("value-" + i);
            positions.add(pos);
        }
        System.out.println("WRITE: " + ITEMS + " IN " + (System.currentTimeMillis() - start));

        long s = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            String val = seg.get(positions.get(i));
        }
        System.out.println("READ: " + ITEMS + " IN " + (System.currentTimeMillis() - s));
    }

    private static Record create(int key, String value) {
        return Record.create(key, KeyParser.INT, value, Serializers.VSTRING, ByteBuffer.allocate(1024));
    }
}