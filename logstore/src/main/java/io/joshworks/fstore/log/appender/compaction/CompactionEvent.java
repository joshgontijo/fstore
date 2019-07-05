package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;

import java.io.File;
import java.util.List;
import java.util.function.Consumer;

class CompactionEvent<T> {
    final List<Log<T>> segments;
    final SegmentCombiner<T> combiner;
    final File segmentFile;
    final SegmentFactory<T> segmentFactory;
    final StorageMode storageMode;
    final BufferPool bufferPool;
    final Serializer<T> serializer;
    final String name;
    final int level;
    final Consumer<CompactionResult<T>> onComplete;
    final int readPageSize;
    final double checksumProbability;


    CompactionEvent(
            List<Log<T>> segments,
            SegmentCombiner<T> combiner,
            File segmentFile,
            SegmentFactory<T> segmentFactory,
            StorageMode storageMode,
            Serializer<T> serializer,
            BufferPool bufferPool,
            String name,
            int level,
            int readPageSize,
            double checksumProbability,
            Consumer<CompactionResult<T>> onComplete) {

        this.segments = segments;
        this.combiner = combiner;
        this.segmentFile = segmentFile;
        this.segmentFactory = segmentFactory;
        this.storageMode = storageMode;
        this.serializer = serializer;
        this.bufferPool = bufferPool;
        this.name = name;
        this.level = level;
        this.readPageSize = readPageSize;
        this.checksumProbability = checksumProbability;
        this.onComplete = onComplete;
    }
}
