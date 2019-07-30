package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.lsmtree.sstable.Entry;

import java.util.function.Function;

import static io.joshworks.eventry.EventUtils.validIndexEntry;

class IndexCompactor extends UniqueMergeCombiner<Entry<IndexKey, Long>> {

    private final Function<Long, StreamMetadata> metadataSupplier;
    private final Function<Long, Integer> versionSupplier;

    IndexCompactor(Function<Long, StreamMetadata> metadataSupplier, Function<Long, Integer> versionSupplier) {
        this.metadataSupplier = metadataSupplier;
        this.versionSupplier = versionSupplier;
    }

    @Override
    public boolean filter(Entry<IndexKey, Long> entry) {
        long stream = entry.key.stream;
        int version = entry.key.version;
        long timestamp = entry.timestamp;

        StreamMetadata metadata = metadataSupplier.apply(stream);
        return validIndexEntry(metadata, version, timestamp, versionSupplier);
    }
}
