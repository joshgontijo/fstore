package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class LastFlushDiscardCombiner implements SegmentCombiner<LogRecord> {

    private final AtomicReference<TransactionLog.FlushTask> lastFlush;

    public LastFlushDiscardCombiner(AtomicReference<TransactionLog.FlushTask> lastFlush) {
        this.lastFlush = lastFlush;
    }

    @Override
    public void merge(List<? extends Log<LogRecord>> segments, Log<LogRecord> output) {
        TransactionLog.FlushTask flushTask = lastFlush.get();
        for (Log<LogRecord> segment : segments) {
            SegmentIterator<LogRecord> iterator = segment.iterator(Direction.FORWARD);
            while (iterator.hasNext()) {
                LogRecord entry = iterator.next();
                if (flushTask.timestamp <= entry.timestamp) {
                    //entry not yet flushed to sstable write to output
                    output.append(entry);
                }
            }
        }
    }
}
