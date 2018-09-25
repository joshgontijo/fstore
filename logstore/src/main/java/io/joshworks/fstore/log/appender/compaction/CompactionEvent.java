package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.DataStream;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.appender.StorageProvider;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.io.File;
import java.util.List;

class CompactionEvent<T, L extends Log<T>> {
    final List<L> segments;
    final SegmentCombiner<T> combiner;
    final File segmentFile;
    final SegmentFactory<T, L> segmentFactory;
    final StorageProvider storageProvider;
    final Serializer<T> serializer;
    final DataStream<T> dataStream;
    final int level;
    final String magic;

    CompactionEvent(List<L> segments, SegmentCombiner<T> combiner, File segmentFile, SegmentFactory<T, L> segmentFactory, StorageProvider storageProvider, Serializer<T> serializer, DataStream<T> dataStream, int level, String magic) {
        this.segments = segments;
        this.combiner = combiner;
        this.segmentFile = segmentFile;
        this.segmentFactory = segmentFactory;
        this.storageProvider = storageProvider;
        this.serializer = serializer;
        this.dataStream = dataStream;
        this.level = level;
        this.magic = magic;
    }
}
