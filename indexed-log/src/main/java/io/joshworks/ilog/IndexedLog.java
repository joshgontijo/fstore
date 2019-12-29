package io.joshworks.ilog;

import io.joshworks.fstore.log.appender.LogAppender;

public class IndexedLog<T>  {

    private final LogAppender<T> delegate;

    public IndexedLog() {
        this.delegate = LogAppender.builder()
    }
}
