package io.joshworks.fstore.lsmtree.log;

import java.util.function.Consumer;

public interface TransactionLog<K extends Comparable<K>, V> {

    void append(Record<K, V> record);

    void markFlushed();

    void restore(Consumer<Record<K, V>> consumer);

    void close();
}
