package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TransactionLogTest {

    private TransactionLog<String, Integer> log;
    private File testDir;

    @Before
    public void setUp() {
        testDir = FileUtils.testFolder();
        log = new TransactionLog<>(testDir, Serializers.VSTRING, Serializers.INTEGER, Size.MB.ofInt(50), 3, "txlog", StorageMode.RAF);
    }

    @After
    public void tearDown() throws Exception {
        log.close();
        FileUtils.tryDelete(testDir);
    }

    @Test
    public void restore_returns_items_not_marked_as_flushed() {

        log.append(LogRecord.add("a", 1));
        log.append(LogRecord.add("b", 2));
        String token = log.markFlushing();

        LogRecord c = LogRecord.add("c", 3);
        log.append(c);
        log.markFlushed(token);

        List<LogRecord> records = notFlushedItems(log);
        assertEquals(1, records.size());
        assertEquals(c, records.get(0));
    }

    @Test
    public void only_successful_flushes_are_considered_when_restoring_log() {

        log.append(LogRecord.add("a", 1));
        log.append(LogRecord.add("b", 2));

        log.markFlushing();

        List<LogRecord> records = notFlushedItems(log);
        assertEquals(2, records.size());
    }

    @Test
    public void only_successful_flushes_are_considered_when_restoring_log2() {

        log.append(LogRecord.add("a", 1));
        log.append(LogRecord.add("b", 2));

        String token = log.markFlushing();
        log.markFlushed(token);

        LogRecord c = LogRecord.add("c", 3);
        log.append(c);

        log.markFlushing();

        List<LogRecord> records = notFlushedItems(log);
        assertEquals(1, records.size());
        assertEquals(c, records.get(0));
    }

    @Test
    public void only_last_flush_is_kept() {

        log.append(LogRecord.add("a", 1));
        String token1 = log.markFlushing();
        log.markFlushed(token1);


        log.append(LogRecord.add("b", 2));
        String token2 = log.markFlushing();
        log.markFlushed(token2);

        List<LogRecord> records = notFlushedItems(log);
        assertEquals(0, records.size());

        TransactionLog.FlushTask flushTask = log.lastCompletedFlush.get();
        assertEquals(token2, flushTask.token);
    }

    @Test
    public void only_last_flush_is_kept_with_incomplete_flushTask() {

        log.append(LogRecord.add("a", 1));
        String token1 = log.markFlushing();
        log.markFlushed(token1);


        log.append(LogRecord.add("b", 2));
        String token2 = log.markFlushing();

        List<LogRecord> records = notFlushedItems(log);
        assertEquals(1, records.size());

        TransactionLog.FlushTask flushTask = log.lastCompletedFlush.get();
        assertEquals(token1, flushTask.token);
    }

    private static List<LogRecord> notFlushedItems(TransactionLog<String, Integer> log) {
        List<LogRecord> records = new ArrayList<>();
        log.restore(records::add);
        return records;
    }
}