package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.compaction.combiner.NoOpCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.lsmtree.EntryType;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

public class TransactionLog<K extends Comparable<K>, V> {

    private final LogAppender<Record<K, V>> appender;
    private long lastFlush;

    public TransactionLog(File root, Serializer<K> keySerializer, Serializer<V> valueSerializer, String name) {
        this.appender = LogAppender.builder(new File(root, "log"), new RecordSerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new NoOpCombiner<>())
                .name(name + "-log")
                .open();
        this.lastFlush = Log.START;
    }

    public long append(Record<K, V> record) {
        return appender.append(record);
    }

    public void markFlushed() {
        appender.append(Record.memFlushed());
    }

    public void restore(Consumer<Record<K, V>> consumer) {
        Deque<Record<K, V>> stack = new ArrayDeque<>();
        try (LogIterator<Record<K, V>> iterator = appender.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                Record<K, V> record = iterator.next();
                if (EntryType.MEM_FLUSHED.equals(record.type)) {
                    break;
                }
                stack.push(record);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        for (Record<K, V> kvRecord : stack) {
            consumer.accept(kvRecord);
        }
    }

    public void close() {
        appender.close();
    }
}
