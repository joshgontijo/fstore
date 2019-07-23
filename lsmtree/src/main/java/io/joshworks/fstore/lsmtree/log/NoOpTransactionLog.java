package io.joshworks.fstore.lsmtree.log;

import java.util.function.Consumer;

public class NoOpTransactionLog<K extends Comparable<K>, V> implements TransactionLog<K, V> {

    @Override
    public void append(LogRecord<K, V> record) {
    }

    @Override
    public void markFlushed() {
    }

    @Override
    public void restore(Consumer<LogRecord<K, V>> consumer) {
    }

    @Override
    public void close() {
    }
}
