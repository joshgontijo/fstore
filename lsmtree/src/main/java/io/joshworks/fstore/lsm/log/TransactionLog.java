package io.joshworks.fstore.lsm.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.appenders.SimpleLogAppender;
import io.joshworks.fstore.log.appender.compaction.combiner.NoOpCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.lsm.EntryType;

import java.io.File;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Stack;
import java.util.function.Consumer;

public class TransactionLog<K extends Comparable<K>, V> {

    private final SimpleLogAppender<Record<K, V>> appender;
    private long lastFlush = Log.START;

    public TransactionLog(File root, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.appender = new SimpleLogAppender<>(LogAppender.builder(new File(root, "log"), new RecordSerializer<>(keySerializer, valueSerializer))
                .compactionStrategy(new NoOpCombiner<>()));
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
