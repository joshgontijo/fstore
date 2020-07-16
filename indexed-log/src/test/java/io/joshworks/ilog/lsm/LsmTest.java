package io.joshworks.ilog.lsm;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LsmTest {

    public static final RowKey RK = RowKey.LONG;
    public static final int BATCH_SIZE = 1000;
    private Lsm lsm;
    private static final int MEM_TABLE_SIZE = 500000;
    private final RecordPool pool = RecordPool.create()
            .batchSize(BATCH_SIZE)
            .build();

    @Before
    public void setUp() {
        lsm = Lsm.create(TestUtils.testFolder(), RK)
                .memTable(MEM_TABLE_SIZE)
                .compactionThreshold(2)
                .sparse(new SnappyCodec(), Size.KB.ofInt(64));
    }

    @After
    public void tearDown() {
        lsm.delete();
        pool.close();
    }

    @Test
    public void append_no_flush() {
        int items = MEM_TABLE_SIZE / 2;
        Records records = RecordUtils.createN(0, items, pool);
        lsm.append(records);

        for (int i = 0; i < items; i++) {
            Record found = lsm.get(keyOf(i));
            assertNotNull(found);
            assertEquals(i, RecordUtils.longKey(found));
        }
    }

    @Test
    public void append_MANY_TEST() {
        long inserted = 0;
        long time = System.currentTimeMillis();
        while (true) {
            Records records = RecordUtils.createN(inserted, BATCH_SIZE, pool);
            lsm.append(records);
            inserted += records.size();
            if (inserted % 1000000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println(inserted + " - " + (now - time) + "ms");
                time = now;
            }
        }
    }


//    @Test
//    public void iterate() {
//        int items = (int) (MEM_TABLE_SIZE * 1.5);
//        Records records = RecordUtils.createN(0, items, pool);
//        lsm.append(records);
//
//        LogIterator it = lsm.logIterator();
//        var dst = Buffers.allocate(8096, false);
//        int entries = 0;
//        long lastKey = -1;
//        while (it.read(dst) > 0) {
//            dst.flip();
//            while (RecordBatch.hasNext(dst)) {
//                long k = dst.getLong(dst.position() + Record.KEY.offset(dst));
//                RecordBatch.advance(dst);
//                assertEquals(lastKey + 1, k);
//                lastKey = k;
//                entries++;
//            }
//            dst.compact();
//        }
//
//        assertEquals(items, entries);
//    }

    @Test
    public void append_flush() {
        int items = (int) (MEM_TABLE_SIZE * 1.5);
        Records records = RecordUtils.createN(0, items, pool);
        lsm.append(records);

        for (int i = 0; i < items; i++) {
            ByteBuffer key = keyOf(i);
            Record found = lsm.get(key);
            assertNotNull("Failed on " + i, found);

            int compare = found.compare(RK, key);
            assertEquals("Keys are not equals", 0, compare);
        }
    }

    @Test
    public void delete() {
        lsm.append(add(0, String.valueOf(0)));
        lsm.append(delete(0));

        var dst = Buffers.allocate(1024, false);
        Record record = lsm.get(keyOf(0));
        assertNotNull(record);
        dst.flip();
        assertTrue(record.hasAttribute(RecordFlags.DELETION_ATTR));
    }

    @Test
    public void update_no_flush_returns_last_entry() {
        lsm.append(add(0, String.valueOf(0)));
        lsm.append(add(0, String.valueOf(1)));

        Record found = lsm.get(keyOf(0));
        assertNotNull(found);
        assertEquals("1", RecordUtils.stringValue(found));
    }

    @Test
    public void update_flush_returns_last_entry() {
        lsm.append(add(0L, String.valueOf(0)));
        lsm.flush();
        lsm.append(add(0, String.valueOf(1)));

        Record found = lsm.get(keyOf(0));
        assertNotNull(found);
        assertEquals("1", RecordUtils.stringValue(found));
    }

    public Records add(long key, String val) {
        Records records = pool.empty();
        records.add(RecordUtils.create(key, val));
        return records;
    }

    public Records delete(long key) {
        Records records = pool.empty();
        records.add(LsmRecordUtils.delete(key));
        return records;
    }

    private static ByteBuffer keyOf(long key) {
        return Buffers.wrap(key);
    }

}