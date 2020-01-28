package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
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
    private int MEM_TABLE_SIZE;

    @Before
    public void setUp() throws Exception {
        MEM_TABLE_SIZE = 500;
        lsm = new Lsm(TestUtils.testFolder(), KeyComparator.LONG, 1024, MEM_TABLE_SIZE, -1);
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
            assertEquals(i, keyValue(dst));
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
            assertTrue(rsize > 0);
            assertEquals(i, keyValue(dst));
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
        assertTrue(LsmRecord.deletion(dst));
    }

    @Test
    public void update_no_flush_returns_last_entry() {
        lsm.append(LsmRecordUtils.add(0, String.valueOf(0)));
        lsm.append(LsmRecordUtils.add(0, String.valueOf(1)));

        var dst = Buffers.allocate(1024, false);
        int rsize = lsm.get(keyOf(0), dst);
        assertTrue(rsize > 0);
        dst.flip();
        assertEquals("1", RecordUtils.readValue(dst, Serializers.STRING));
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
        assertEquals("1", RecordUtils.readValue(dst, Serializers.STRING));
    }

    private static ByteBuffer keyOf(long key) {
        return Buffers.allocate(Long.BYTES, false).putLong(key).flip();
    }

    private static long keyValue(ByteBuffer buffer) {
        return RecordUtils.readKey(buffer, Serializers.LONG);
    }
}