package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Records;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class IndexedSegmentTest extends SegmentTest {

    @Override
    protected Segment open(String fileName) {
        return new IndexedSegment(TestUtils.testFile(fileName), pool, RowKey.LONG, 100000);
    }

    @Override
    protected void getAll(int items) {
        IndexedSegment is = (IndexedSegment) segment;
        for (long i = 0; i < items; i++) {
            Records rec = is.get(Buffers.wrap(i), IndexFunction.EQUALS);
            assertNotNull(rec);
            assertFalse(rec.isEmpty());
            assertEquals(i, RecordUtils.longKey(rec.get(0)));
        }
    }

}