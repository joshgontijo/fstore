package io.joshworks.fstore.log.appender.compaction;

import java.io.Closeable;

public interface ICompactor extends Closeable {
    void compact();
}
