package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.compaction.combiner.DiscardCombiner;
import io.joshworks.fstore.lsmtree.EntryType;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

public class PersistentTransactionLog<K extends Comparable<K>, V> implements TransactionLog {

    private final LogAppender<LogRecord> appender;

    public PersistentTransactionLog(File root, Serializer<K> keySerializer, Serializer<V> valueSerializer, String name, StorageMode mode) {
        //TODO when segment rolls, then sstable must be flushed, otherwise the log compactor might delete entries that are
        //not persisted to disk yet
        this.appender = LogAppender.builder(new File(root, "log"), new LogRecordSerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new DiscardCombiner<>())
                .name(name + "-log")
                .storageMode(mode)
                .open();
    }

    @Override
    public synchronized long append(LogRecord record) {
        return appender.append(record);
    }

    @Override
    public synchronized void markFlushed(long position) {
        appender.append(LogRecord.memFlushed(position));
    }

    @Override
    public void restore(Consumer<LogRecord> consumer) {
        Deque<LogRecord> stack = new ArrayDeque<>();
        try (LogIterator<LogRecord> iterator = appender.iterator(Direction.BACKWARD)) {
            long lastFlush = -1;
            while (iterator.hasNext() && iterator.position() > lastFlush) {
                LogRecord record = iterator.next();
                if (EntryType.MEM_FLUSHED.equals(record.type)) {
                    IndexFlushed flushed = (IndexFlushed) record;
                    lastFlush = flushed.position;
                }
                stack.push(record);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (LogRecord kvRecord : stack) {
            consumer.accept(kvRecord);
        }
    }

    @Override
    public void close() {
        appender.close();
    }
}
