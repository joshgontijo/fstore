package io.joshworks.ilog;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.VStringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RandomAccessTest {

    private IndexedSegment segment;
    private ByteBuffer writeBuffer = Buffers.allocate(4096, false);

    private static int ITEMS = 5000000;

    @Before
    public void setUp() throws IOException {
        segment = new IndexedSegment(TestUtils.testFile(), Size.GB.ofInt(1), false);
    }

    @After
    public void tearDown() {
        segment.delete();
    }

    @Test
    public void write() {
        long start = System.currentTimeMillis();
        List<Long> offsets = new ArrayList<>();
        for (long i = 0; i < ITEMS; i++) {
            segment.append(create("value-" + i, i), writeBuffer);
            offsets.add(i);
            writeBuffer.clear();
        }
        System.out.println("WRITE: " + ITEMS + " IN " + (System.currentTimeMillis() - start));

        long s = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            Record rec = segment.read(offsets.get(i));
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

    private static Record create(String value, long offset) {
        var bb = ByteBuffer.wrap(VStringSerializer.toBytes(value));
        return Record.create(bb, offset);
    }
}