package io.joshworks.fstore.lsmtree.log;

import java.util.function.Consumer;

public interface TransactionLog<K extends Comparable<K>, V> {

    void append(LogRecord<K, V> record);

    void markFlushed();

    void restore(Consumer<LogRecord<K, V>> consumer);

    void close();
}
