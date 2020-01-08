package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class IndexedSegmentTest {

    private IndexedSegment segment;
    private Index index;

    @Before
    public void setUp() {
        index = new LongIndex(TestUtils.testFile(), Size.MB.ofInt(500));
        segment = new IndexedSegment(TestUtils.testFile(), index);
    }

    @After
    public void tearDown() throws IOException {
        segment.delete();
    }

    @Test
    public void write() throws IOException {
        long start = System.currentTimeMillis();
        int items = 20000000;
        for (long i = 0; i < items; i++) {
            Record record = create(i, "value-" + i);
            segment.append(record);
        }

        System.out.println("WRITE: " + items + " IN " + (System.currentTimeMillis() - start));

        readAll();
    }

    private void readAll() {
        long s = System.currentTimeMillis();
        RecordBatchIterator recordIterator = segment.iterator(bufferOf(0), 4096);
        long idx = 0;
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            int keyLen = record.keyLength();
            long key = Serializers.LONG.fromBytes(record.key());
            assertEquals(Long.BYTES, keyLen);
            assertEquals(idx, key);
            idx++;
        }
        System.out.println("READ: " + idx + " IN " + (System.currentTimeMillis() - s));
    }

    private static Record create(long key, String value) {
        return Record.create(key, Serializers.LONG, value, Serializers.VSTRING, ByteBuffer.allocate(64));
    }

    private static ByteBuffer bufferOf(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).flip();
    }

}