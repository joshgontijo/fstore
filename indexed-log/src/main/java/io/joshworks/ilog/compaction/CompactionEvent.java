package io.joshworks.ilog.compaction;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;

import java.util.List;
import java.util.function.Consumer;

class CompactionEvent<T extends IndexedSegment> {
    View<T> view;
    final List<T> segments;
    final int level;
    final Consumer<CompactionResult<T>> onComplete;
    final SegmentCombiner combiner;

    CompactionEvent(
            View<T> view,
            List<T> segments,
            SegmentCombiner combiner,
            int level,
            Consumer<CompactionResult<T>> onComplete) {

        this.view = view;
        this.segments = segments;
        this.combiner = combiner;
        this.level = level;
        this.onComplete = onComplete;
    }
}
