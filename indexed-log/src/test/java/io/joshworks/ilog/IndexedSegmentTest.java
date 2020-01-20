package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class IndexedSegmentTest {

    private IndexedSegment segment;
    private Index index;
    private BufferPool pool = BufferPool.unpooled(4096, false);

    @Before
    public void setUp() {
        index = new Index(TestUtils.testFile(), Size.MB.ofInt(500), KeyComparator.LONG);
        segment = new IndexedSegment(TestUtils.testFile(), index);
    }

    @After
    public void tearDown() {
        segment.delete();
    }

    @Test
    public void write() {
        int items = 10000;
        for (long i = 0; i < items; i++) {
            Record record = create(i, "value-" + i);
            segment.append(record);
        }
        readAll();
    }

    private void readAll() {
        RecordBatchIterator recordIterator = new RecordBatchIterator(segment, IndexedSegment.START, pool);
        long idx = 0;
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            int keyLen = record.keySize();
            long key = Serializers.LONG.fromBytes(record.key());
            assertEquals(Long.BYTES, keyLen);
            assertEquals(idx, key);
            idx++;
        }
    }

    private static Record create(long key, String value) {
        return Record.create(key, Serializers.LONG, value, Serializers.STRING, ByteBuffer.allocate(64));
    }

    private static ByteBuffer bufferOf(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).flip();
    }

}