package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;
import java.util.function.Supplier;

/**
 * Combiner that copies entries that haven't been flushed to SSTable into a new segment file
 * Timestamp of the last entry of the last flush is used, therefore is possible that it
 * will copy some items that were flushed to sstables, which i acceptable.
 */
class LastFlushedCombiner implements SegmentCombiner<LogRecord> {

    private final Supplier<Long> lastFlushTimeSupplier;

    LastFlushedCombiner(Supplier<Long> lastFlushTimeSupplier) {
        this.lastFlushTimeSupplier = lastFlushTimeSupplier;
    }

    @Override
    public void merge(List<? extends Log<LogRecord>> segments, Log<LogRecord> output) {
        long lastFlushedTime = lastFlushTimeSupplier.get();
        for (Log<LogRecord> seg : segments) {
            SegmentIterator<LogRecord> iterator = seg.iterator(Direction.FORWARD);
            while (iterator.hasNext()) {
                LogRecord record = iterator.next();
                if (record.timestamp >= lastFlushedTime) {
                    output.append(record);
                }
            }
        }
    }
}
