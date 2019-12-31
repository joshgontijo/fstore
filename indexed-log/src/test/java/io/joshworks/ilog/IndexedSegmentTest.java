package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
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
        while (offset < 10000) {
            segment.append(create("value-" + offset, offset), writeBuffer);
            writeBuffer.clear();
            offset++;
        }
        System.out.println("WRITE: " + offset + " IN " + (System.currentTimeMillis() - start));

        readAll();

//        segment.read(0, 4096).forEach(System.out::println);

    }

    private void readAll() {
        long offset = 2;
        RecordIterator recordIterator = segment.iterator(offset);
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            System.out.println(record);
        }
    }

    private static Record create(String value, long offset) {
        var bb = ByteBuffer.wrap(VStringSerializer.toBytes(value));
        return Record.create(bb, offset);
    }
}