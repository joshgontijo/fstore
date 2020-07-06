package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;

import java.util.List;

import static io.joshworks.fstore.core.io.Channels.transferFully;

public class ConcatenateCombiner implements SegmentCombiner {

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        segments.stream()
                .map(IndexedSegment::channel)
                .forEach(c -> transferFully(c, output.channel()));

        output.reindex();
    }

}
