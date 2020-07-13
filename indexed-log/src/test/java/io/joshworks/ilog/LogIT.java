package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.Lsm;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.junit.Test;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.wrap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class LogIT {

    private static final int memTableSize = 1000;

    private static RecordPool pool = RecordPool.create()
            .batchSize(memTableSize + 1)
            .build();


    @Test
    public void appTest() {

        final Lsm lsm = Lsm.create(TestUtils.testFolder(), RowKey.LONG)
                .memTable(memTableSize, Size.MB.ofInt(50))
                .compactionThreads(1)
                .compactionThreshold(5)
                .open();

        Records records = pool.empty();
        for (int i = 0; i < memTableSize + 1; i++) {
            records.add(RecordUtils.create(i, "value-" + i));
        }
        lsm.append(records);

        for (long i = 0; i < memTableSize + 1; i++) {
            ByteBuffer key = wrap(i);
            Record found = lsm.get(key);

            assertNotNull(found);

            int compare = found.compare(RowKey.LONG, key);
            assertEquals(0, compare);
        }
        lsm.close();
    }


}
