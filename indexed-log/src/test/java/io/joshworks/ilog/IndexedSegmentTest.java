package io.joshworks.ilog;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.VStringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IndexedSegmentTest {

    private IndexedSegment segment;
    private ByteBuffer writeBuffer = Buffers.allocate(4096, false);

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
        int offset = 0;
        while (offset < 10000000) {
            segment.append(create("value-" + offset, offset), writeBuffer);
            writeBuffer.clear();
            offset++;
        }
        System.out.println("WRITE: " + offset + " IN " + (System.currentTimeMillis() - start));

        readAll();
    }

    @Test
    public void write2() {
        Segment<String> seg = new Segment<>(TestUtils.testFile(), StorageMode.RAF, Size.GB.of(1), Serializers.VSTRING, new SimpleBufferPool("a", 4096, false), WriteMode.LOG_HEAD, 1);
        long start = System.currentTimeMillis();
        int offset = 0;
        while (offset < 100000) {
            seg.append("value-" + offset);
            offset++;
        }
        System.out.println("WRITE: " + offset + " IN " + (System.currentTimeMillis() - start));

        long s = System.currentTimeMillis();
        int items = 0;
        SegmentIterator<String> it = seg.iterator(Direction.FORWARD);
        while (it.hasNext()) {
            String next = it.next();
            items++;
        }
        System.out.println("READ: " + items + " IN " + (System.currentTimeMillis() - s));

//        readAll();
    }

    private void readAll() {
        long s = System.currentTimeMillis();
        RecordIterator recordIterator = segment.batch(0, 4096);
        int items = 0;
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            items++;
//            System.out.println(record);
        }
        System.out.println("READ: " + items + " IN " + (System.currentTimeMillis() - s));
    }

    private static Record create(String value, long offset) {
        var bb = ByteBuffer.wrap(VStringSerializer.toBytes(value));
        return Record.create(bb, offset);
    }
}