package io.joshworks.ilog.lsm;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LsmTest {

    private Lsm lsm;
    private static final int MEM_TABLE_SIZE = 500;

    @Before
    public void setUp() {
        lsm = Lsm.create(TestUtils.testFolder(), KeyComparator.LONG)
                .memTable(MEM_TABLE_SIZE, Size.MB.ofInt(10), false)
                .codec(new SnappyCodec())
                .open();

    }

    @After
    public void tearDown() {
        lsm.delete();
    }

    @Test
    public void append_no_flush() {
        int items = MEM_TABLE_SIZE / 2;
        for (int i = 0; i < items; i++) {
            lsm.append(LsmRecordUtils.add(i, String.valueOf(i)));
        }

        for (int i = 0; i < items; i++) {
            var dst = Buffers.allocate(1024, false);
            int rsize = lsm.get(keyOf(i), dst);
            dst.flip();
            assertTrue(rsize > 0);
            assertEquals(i, RecordUtils.readKey(dst));
        }
    }

    @Test
    public void append_flush() {
        int items = (int) (MEM_TABLE_SIZE * 1.5);
        for (int i = 0; i < items; i++) {
            lsm.append(LsmRecordUtils.add(i, String.valueOf(i)));
        }

        for (int i = 0; i < items; i++) {
            var dst = Buffers.allocate(1024, false);
            int rsize = lsm.get(keyOf(i), dst);
            dst.flip();
            assertTrue("Failed on " + i, rsize > 0);
            assertTrue("Failed on " + i, Record.isValid(dst));

            System.out.println(Record.toString(dst));

        }
    }

    @Test
    public void delete() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.append(LsmRecordUtils.delete(0));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertTrue(Record.hasAttribute(dst, RecordFlags.DELETION_ATTR));
    }

    @Test
    public void update_no_flush_returns_last_entry() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.append(LsmRecordUtils.add(0, String.valueOf(1)));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertEquals("1", RecordUtils.readValue(dst));
    }

    @Test
    public void update_flush_returns_last_entry() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.flush();
        lsm.append(LsmRecordUtils.add(0, String.valueOf(1)));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertEquals("1", RecordUtils.readValue(dst));
    }

    private static ByteBuffer keyOf(long key) {
        return Buffers.allocate(Long.BYTES, false).putLong(key).flip();
    }

}