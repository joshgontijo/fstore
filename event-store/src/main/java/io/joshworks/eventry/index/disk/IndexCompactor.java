package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;
import java.util.function.Function;

public class IndexCompactor extends UniqueMergeCombiner<IndexEntry> {

    private final Function<Long, StreamMetadata> streamSupplier;

    public IndexCompactor(Function<Long, StreamMetadata> streamSupplier) {
        this.streamSupplier = streamSupplier;
    }

    @Override
    public void merge(List<? extends Log<IndexEntry>> segments, Log<IndexEntry> output) {
//        BloomFilter
        IndexSegment indexSegment = (IndexSegment) output;
        long totalEntries = segments.stream().mapToLong(Log::entries).sum();
        indexSegment.newBloomFilter(totalEntries);
        super.merge(segments, output);
    }

    @Override
    public boolean filter(IndexEntry entry) {
        StreamMetadata metadata = streamSupplier.apply(entry.stream);
        return !metadata.truncated() || metadata.truncated <= entry.version;
    }
}
