package io.joshworks.ilog.compaction;

import io.joshworks.ilog.Segment;

import java.util.List;

class CompactionResult<T extends Segment> {

    final Exception exception;
    final List<T> sources;
    final T target; //nullable
    final int level;

    private CompactionResult(List<T> segments, T target, int level, Exception exception) {
        this.exception = exception;
        this.sources = segments;
        this.target = target;
        this.level = level;
    }

    static <T extends Segment> CompactionResult<T> success(List<T> segments, T target, int level) {
        return new CompactionResult<>(segments, target, level, null);
    }

    static <T extends Segment> CompactionResult<T> failure(List<T> segments, T target, int level, Exception exception) {
        return new CompactionResult<>(segments, target, level, exception);
    }

    boolean successful() {
        return exception == null;
    }
}
