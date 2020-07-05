package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.ChannelUtil;
import io.joshworks.ilog.IndexedSegment;

import java.util.List;

public class ConcatenateCombiner implements SegmentCombiner {

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        for (IndexedSegment segment : segments) {
            ChannelUtil.transferFully(segment.channel(), output.channel());
        }
    }
}
