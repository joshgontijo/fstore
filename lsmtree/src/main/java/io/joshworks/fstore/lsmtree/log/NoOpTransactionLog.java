package io.joshworks.fstore.lsmtree.log;

import java.util.function.Consumer;

public class NoOpTransactionLog<K extends Comparable<K>, V> implements TransactionLog {

    @Override
    public long append(LogRecord record) {
        return -1;
    }

    @Override
    public void markFlushed(long position) {
    }

    @Override
    public void restore(Consumer<LogRecord> consumer) {
    }

    @Override
    public void close() {
    }
}
