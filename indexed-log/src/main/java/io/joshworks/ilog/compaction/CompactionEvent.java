package io.joshworks.ilog.compaction;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;

import java.util.List;
import java.util.function.Consumer;

class CompactionEvent {
    View view;
    final List<IndexedSegment> segments;
    final int level;
    final Consumer<CompactionResult> onComplete;
    final SegmentCombiner combiner;

    CompactionEvent(
            View view,
            List<IndexedSegment> segments,
            SegmentCombiner combiner,
            int level,
            Consumer<CompactionResult> onComplete) {

        this.view = view;
        this.segments = segments;
        this.combiner = combiner;
        this.level = level;
        this.onComplete = onComplete;
    }
}
