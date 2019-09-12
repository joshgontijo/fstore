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

public class PersistentTransactionLog<K extends Comparable<K>, V> implements TransactionLog<K, V> {

    private final LogAppender<LogRecord<K, V>> appender;

    public PersistentTransactionLog(File root, Serializer<K> keySerializer, Serializer<V> valueSerializer, String name, StorageMode mode) {
        //TODO when segment roll, then sstable must be flushed, otherwise the log compactor might delete entries that are
        //not persisted to disk yet
        this.appender = LogAppender.builder(new File(root, "log"), new RecordSerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new DiscardCombiner<>())
                .name(name + "-log")
                .storageMode(mode)
                .open();
    }

    @Override
    public void append(LogRecord<K, V> record) {
        appender.append(record);
    }

    @Override
    public void markFlushed() {
        appender.append(LogRecord.memFlushed());
    }

    @Override
    public void restore(Consumer<LogRecord<K, V>> consumer) {
        Deque<LogRecord<K, V>> stack = new ArrayDeque<>();
        try (LogIterator<LogRecord<K, V>> iterator = appender.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                LogRecord<K, V> record = iterator.next();
                if (EntryType.MEM_FLUSHED.equals(record.type)) {
                    break;
                }
                stack.push(record);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (LogRecord<K, V> kvRecord : stack) {
            consumer.accept(kvRecord);
        }
    }

    @Override
    public void close() {
        appender.close();
    }
}
