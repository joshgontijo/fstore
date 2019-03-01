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

    // -------------- IndexAppenderTest#version_with_multiple_segments_returns_correct_version ------------
    //FIXME java.lang.NullPointerException
    //	at io.joshworks.eventry.index.disk.IndexCompactor.filter(IndexCompactor.java:31)
    //	at io.joshworks.eventry.index.disk.IndexCompactor.filter(IndexCompactor.java:11)
    //	at io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner.mergeItems(UniqueMergeCombiner.java:34)
    //	at io.joshworks.fstore.log.appender.compaction.combiner.MergeCombiner.merge(MergeCombiner.java:23)
    //	at io.joshworks.eventry.index.disk.IndexCompactor.merge(IndexCompactor.java:25)
    //	at io.joshworks.fstore.log.appender.compaction.CompactionTask.onEvent(CompactionTask.java:61)
    //	at io.joshworks.fstore.core.seda.StageHandler.handle(StageHandler.java:8)
    //	at io.joshworks.fstore.core.seda.Stage.lambda$submit$0(Stage.java:53)
    //	at io.joshworks.fstore.core.seda.BoundedExecutor.lambda$execute$0(BoundedExecutor.java:51)
    //	at io.joshworks.fstore.core.seda.SedaTask.run(SedaTask.java:22)
    //	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
    //	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
    //	at java.base/java.lang.Thread.run(Thread.java:834)
    //[compaction-cleanup-1] ERROR compactor [index] - Compaction error
    @Override
    public boolean filter(IndexEntry entry) {
        StreamMetadata metadata = streamSupplier.apply(entry.stream);
        return !metadata.truncated() || metadata.truncateBefore <= entry.version;
    }
}
