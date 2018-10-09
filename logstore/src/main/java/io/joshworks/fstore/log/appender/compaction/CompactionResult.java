package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.log.segment.Log;

import java.util.List;

class CompactionResult<T> {

    final Exception exception;
    final List<Log<T>> sources;
    final Log<T> target; //nullable
    final int level;

    private CompactionResult(List<Log<T>> segments, Log<T> target, int level, Exception exception) {
        this.exception = exception;
        this.sources = segments;
        this.target = target;
        this.level = level;
    }

    static <T> CompactionResult<T> success(List<Log<T>> segments, Log<T> target, int level) {
        return new CompactionResult<>(segments, target, level, null);
    }

    static <T> CompactionResult<T> failure(List<Log<T>> segments, Log<T> target, int level, Exception exception) {
        return new CompactionResult<>(segments, target, level, exception);
    }

    boolean successful() {
        return exception == null;
    }
}
