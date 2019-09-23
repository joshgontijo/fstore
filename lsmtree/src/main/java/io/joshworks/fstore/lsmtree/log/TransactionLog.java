package io.joshworks.fstore.lsmtree.log;

import java.util.function.Consumer;

public interface TransactionLog {

    long append(LogRecord record);

    void markFlushed(long position);

    void restore(Consumer<LogRecord> consumer);

    void close();
}
