package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.SegmentIterator;
import io.joshworks.ilog.compaction.PeekingIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class MergeCombiner implements SegmentCombiner {

    public static final int BATCH_SIZE = 4096; //TODO

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {

        List<PeekingIterator> iterators = segments.stream()
                .map(s -> s.iterator(BATCH_SIZE))
                .map(PeekingIterator::new)
                .collect(Collectors.toList());

        try {
            mergeItems(new ArrayList<>(iterators), output);
        } finally {
            for (SegmentIterator iterator : iterators) {
                IOUtils.closeQuietly(iterator);
            }
        }
    }

    public abstract void mergeItems(List<PeekingIterator> items, IndexedSegment output);
}
