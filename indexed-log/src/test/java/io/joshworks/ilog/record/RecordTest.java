package io.joshworks.ilog.record;


import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.index.RowKey;
import org.junit.Test;

import static io.joshworks.ilog.RecordUtils.create;
import static io.joshworks.ilog.RecordUtils.longKey;
import static io.joshworks.ilog.RecordUtils.stringValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RecordTest {

    @Test
    public void testValidRecord() {
        long key = 1;
        String value = "abc";
        Record rec = create(key, value);

        assertTrue(rec.isValid());
    }

    @Test
    public void testKey() {
        long key = 1;
        String value = "abc";
        Record rec = create(key, value);

        assertEquals(Long.BYTES, rec.keyLen());
        assertEquals(key, longKey(rec));
    }

    @Test
    public void testValue() {
        long key = 1;
        String value = "abc";
        Record rec = create(key, value);

        assertEquals(value.length(), rec.valueSize());
        assertEquals(value, stringValue(rec));
    }

    @Test
    public void timestamp() {
        long key = 1;
        String value = "abc";
        int precision = 10000;
        Record rec = create(key, value);

        assertEquals(System.currentTimeMillis() / precision, rec.timestamp() / precision);
    }

    @Test
    public void defaultSequence() {
        Record rec = create(1, "abc");
        assertEquals(0, rec.sequence());
    }

    @Test
    public void compareKeys() {
        int key = 1;
        Record rec = create(key, "abc");

        assertEquals(1, rec.compare(RowKey.LONG, Buffers.wrap(0L)));
        assertEquals(0, rec.compare(RowKey.LONG, Buffers.wrap(1L)));
        assertEquals(-1, rec.compare(RowKey.LONG, Buffers.wrap(2L)));
    }
}