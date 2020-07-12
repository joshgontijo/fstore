package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ThreadLocalRandom;

import static io.joshworks.ilog.RecordUtils.longKey;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexedSegmentTest {

    private IndexedSegment segment;
    private RecordPool pool = RecordPool.create()
            .batchSize(10000)
            .build();
    private String fileName;

    @Before
    public void setUp() {
        int randLevel = ThreadLocalRandom.current().nextInt(0, 99);
        long randIdx = ThreadLocalRandom.current().nextLong(0, 100000000);
        fileName = LogUtil.segmentFileName(randIdx, randLevel);
        segment = open(fileName);
    }

    private IndexedSegment open(String fileName) {
        return new IndexedSegment(TestUtils.testFile(fileName), 100000, RowKey.LONG, pool);
    }

    @After
    public void tearDown() {
        segment.delete();
    }

    @Test
    public void size() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);

        assertEquals(items, appended);
        assertEquals(items, segment.entries());
    }

    @Test
    public void append() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);

        assertEquals(items, appended);
        iterateAll(items);
        getAll(items);
    }

    @Test
    public void reopen() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);
        assertEquals(items, appended);

        segment.close();
        segment = open(fileName);

        iterateAll(items);
    }

    @Test
    public void reindex() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);
        assertEquals(items, appended);

        segment.reindex();

        assertEquals(items, segment.entries());
        iterateAll(items);
        getAll(items);
    }

    @Test
    public void reindex_after_reopening() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);
        assertEquals(items, appended);

        segment.close();
        segment = open(fileName);
        segment.reindex();

        assertEquals(items, segment.entries());
        iterateAll(items);
        getAll(items);
    }

    @Test
    public void reopen_rolled_segment() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);
        assertEquals(items, appended);

        segment.roll();
        segment.close();
        segment = open(fileName);

        assertEquals(items, segment.entries());
        iterateAll(items);
        getAll(items);
    }

    @Test
    public void rolled_segment_is_marked_as_readOnly() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);
        assertEquals(items, appended);

        segment.roll();
        assertTrue(segment.readOnly());
    }

    @Test
    public void reopened_rolled_segment_is_marked_as_readOnly() {
        int items = 10000;
        Records records = RecordUtils.createN(0, items, pool);
        int appended = segment.append(records, 0);
        assertEquals(items, appended);

        segment.roll();
        segment.close();
        segment = open(fileName);
        assertTrue(segment.readOnly());
    }

    private void getAll(int items) {
        for (long i = 0; i < items; i++) {
            Records rec = segment.get(Buffers.wrap(i), IndexFunction.EQUALS);
            assertNotNull(rec);
            assertFalse(rec.isEmpty());
            assertEquals(i, RecordUtils.longKey(rec.get(0)));
        }
    }

    private void iterateAll(int items) {
        SegmentIterator recordIterator = new SegmentIterator(segment, Log.START, 4096, pool);
        long idx = 0;
        while (recordIterator.hasNext()) {
            Record rec = recordIterator.next();
            assertEquals(Long.BYTES, rec.keyLen());
            assertEquals(idx, longKey(rec));
            idx++;
        }

        assertEquals(items, idx);
    }

}