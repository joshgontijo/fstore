package io.joshworks.ilog.compaction;

import io.joshworks.ilog.IndexedSegment;

import java.util.List;

class CompactionResult {

    final Exception exception;
    final List<IndexedSegment> sources;
    final IndexedSegment target; //nullable
    final int level;

    private CompactionResult(List<IndexedSegment> segments, IndexedSegment target, int level, Exception exception) {
        this.exception = exception;
        this.sources = segments;
        this.target = target;
        this.level = level;
    }

    static CompactionResult success(List<IndexedSegment> segments, IndexedSegment target, int level) {
        return new CompactionResult(segments, target, level, null);
    }

    static CompactionResult failure(List<IndexedSegment> segments, IndexedSegment target, int level, Exception exception) {
        return new CompactionResult(segments, target, level, exception);
    }

    boolean successful() {
        return exception == null;
    }
}
