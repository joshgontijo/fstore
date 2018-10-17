package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.StorageProvider;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;

import java.io.File;
import java.util.List;

class CompactionEvent<T> {
    final List<Log<T>> segments;
    final SegmentCombiner<T> combiner;
    final File segmentFile;
    final SegmentFactory<T> segmentFactory;
    final StorageProvider storageProvider;
    final Serializer<T> serializer;
    final IDataStream dataStream;
    final String name;
    final int level;
    final String magic;

    CompactionEvent(List<Log<T>> segments, SegmentCombiner<T> combiner, File segmentFile, SegmentFactory<T> segmentFactory, StorageProvider storageProvider, Serializer<T> serializer, IDataStream dataStream, String name, int level, String magic) {
        this.segments = segments;
        this.combiner = combiner;
        this.segmentFile = segmentFile;
        this.segmentFactory = segmentFactory;
        this.storageProvider = storageProvider;
        this.serializer = serializer;
        this.dataStream = dataStream;
        this.name = name;
        this.level = level;
        this.magic = magic;
    }
}
