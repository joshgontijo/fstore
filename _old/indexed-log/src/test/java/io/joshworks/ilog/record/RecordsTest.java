package io.joshworks.ilog.record;

import io.joshworks.ilog.RecordUtils;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.joshworks.ilog.RecordUtils.create;
import static io.joshworks.ilog.RecordUtils.recordBuffer;
import static org.junit.Assert.assertEquals;

public class RecordsTest {

    private RecordPool pool;

    @Before
    public void setUp() {
        pool = RecordPool.create().build();
    }

    @Test
    public void addRecord() {
        long key = 1;
        Records records = pool.empty();
        Record rec = create(1, "value");
        records.add(rec);

        assertEquals(1, records.size());
        assertEquals(key, RecordUtils.longKey(records.get(0)));
    }

    @Test
    public void fromBuffer() {
        long key = 1;
        Records records = pool.empty();
        ByteBuffer recBuffer = recordBuffer(key, "value");
        records.add(recBuffer);

        assertEquals(1, records.size());
        assertEquals(key, RecordUtils.longKey(records.get(0)));
    }
}