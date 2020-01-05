package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
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

    public static final KeyParser<Integer> KEY_PARSER = KeyParser.INT;
    private IndexedSegment<Integer> segment;

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
        int items = 20000000;
        var bb = Buffers.allocate(64, false);
        for (int i = 0; i < items; i++) {
            Record record = create(i, "value-" + i, bb);
            segment.append(record);
            bb.clear();
        }

        System.out.println("WRITE: " + items + " IN " + (System.currentTimeMillis() - start));

        readAll();
    }

    private void readAll() {
        long s = System.currentTimeMillis();
        RecordBatchIterator recordIterator = segment.batch(0, 4096);
        int idx = 0;
        while (recordIterator.hasNext()) {
            Record record = recordIterator.next();
            int keyLen = record.keyLength();
            Integer key = KEY_PARSER.readFrom(record.key());
            assertEquals(Integer.BYTES, keyLen);
            assertEquals(Integer.valueOf(idx), key);
            idx++;
//            System.out.println(record);
        }
        System.out.println("READ: " + idx + " IN " + (System.currentTimeMillis() - s));
    }

    private static Record create(int key, String value) {
        return Record.create(key, KEY_PARSER, value, Serializers.VSTRING, ByteBuffer.allocate(64));
    }

    private static Record create(int key, String value, ByteBuffer buffer) {
        return Record.create2(key, KEY_PARSER, value, Serializers.VSTRING, buffer);
    }
}