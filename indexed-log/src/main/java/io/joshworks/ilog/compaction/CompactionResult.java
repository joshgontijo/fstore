package io.joshworks.ilog.compaction;

import io.joshworks.ilog.Segment;
import io.joshworks.ilog.View;

import java.util.List;

class CompactionResult {

    final View view;
    final Exception exception;
    final List<Segment> sources;
    final Segment target; //nullable
    final int level;
    final State state;

    private CompactionResult(View view, State state, List<Segment> segments, Segment target, int level, Exception exception) {
        this.view = view;
        this.state = state;
        this.exception = exception;
        this.sources = segments;
        this.target = target;
        this.level = level;
    }

    static <T extends Segment> CompactionResult success(View view, List<Segment> segments, T target, int level) {
        return new CompactionResult(view, State.COMPLETED, segments, target, level, null);
    }

    static CompactionResult failure(View view, List<Segment> segments, Segment target, int level, Exception exception) {
        return new CompactionResult(view, State.FAILED, segments, target, level, exception);
    }

    static CompactionResult aborted(View view, List<Segment> segments, int level) {
        return new CompactionResult(view, State.ABORTED, segments, null, level, null);
    }

    State state() {
        return state;
    }

    enum State {
        COMPLETED, FAILED, ABORTED
    }
}
