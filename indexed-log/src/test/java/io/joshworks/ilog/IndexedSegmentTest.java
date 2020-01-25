package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class IndexedSegmentTest {

    private IndexedSegment segment;
    private BufferPool pool = BufferPool.unpooled(4096, false);

    @Before
    public void setUp() {
        segment = new IndexedSegment(TestUtils.testFile(LogUtil.segmentFileName(0, 0)), Size.MB.ofInt(500), KeyComparator.LONG);
    }

    @After
    public void tearDown() {
        segment.delete();
    }

    @Test
    public void write() {
        int items = 10000;
        for (long i = 0; i < items; i++) {
            ByteBuffer record = create(i, "value-" + i);
            segment.append(record);
        }
        readAll();
    }

    private void readAll() {
        RecordBatchIterator recordIterator = new RecordBatchIterator(segment, IndexedSegment.START, pool);
        long idx = 0;
        while (recordIterator.hasNext()) {
            ByteBuffer record = recordIterator.next();
            int size = Record2.validate(record);
            int keySize = Record2.keySize(record);
            long key = RecordUtils.readKey(record, Serializers.LONG);
            assertEquals(Long.BYTES, keySize);
            assertEquals(idx, key);
            idx++;
        }
    }

    private static ByteBuffer create(long key, String value) {
        return RecordUtils.create(key, Serializers.LONG, value, Serializers.STRING);
    }

    private static ByteBuffer bufferOf(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).flip();
    }

}