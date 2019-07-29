package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class CompactionTask<T> implements Runnable {

    private final Logger logger;

    private final int level;
    private final File segmentFile;
    private final SegmentCombiner<T> combiner;
    private final List<Log<T>> segments;
    private final Serializer<T> serializer;
    private final StorageMode storageMode;
    private final SegmentFactory<T> segmentFactory;
    private final BufferPool bufferPool;
    private final Consumer<CompactionResult<T>> onComplete;
    private final double checksumProbability;
    private final int readPageSize;

    public CompactionTask(CompactionEvent<T> event) {
        this.logger = Logging.namedLogger(event.name, "compaction-task-" + event.level);
        this.level = event.level;
        this.segmentFile = event.segmentFile;
        this.combiner = event.combiner;
        this.segments = new ArrayList<>(event.segments);
        this.serializer = event.serializer;
        this.storageMode = event.storageMode;
        this.segmentFactory = event.segmentFactory;
        this.bufferPool = event.bufferPool;
        this.onComplete = event.onComplete;
        this.checksumProbability = event.checksumProbability;
        this.readPageSize = event.readPageSize;
    }

    @Override
    public void run() {
        Log<T> output = null;
        try {
            long newSegmentLogSize = segments.stream().mapToLong(this::totalLogicalSize).sum();

            String names = Arrays.toString(segments.stream().map(Log::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment computed size: {}", names, level, combiner.getClass().getSimpleName(), newSegmentLogSize);

            for (int i = 0; i < segments.size(); i++) {
                Log<T> segment = segments.get(i);
                logSegmentInfo(String.valueOf(i), segment);
            }

            long start = System.currentTimeMillis();

            output = segmentFactory.createOrOpen(segmentFile, storageMode, newSegmentLogSize, serializer, bufferPool, WriteMode.MERGE_OUT, checksumProbability, readPageSize);

            combiner.merge(segments, output);
            output.flush();

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            logger.info("Result segment {}: physicalSize: {}, logicalSize: {}, entries: {}", output.name(), output.physicalSize(), output.logicalSize(), output.entries());

            onComplete.accept(CompactionResult.success(segments, output, level));

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            onComplete.accept(CompactionResult.failure(segments, output, level, e));
        }
    }

    private void logSegmentInfo(String id, Log<T> segment) {
        logger.info("Segment[{}] {} - physicalSize: {}, logicalSize: {}, actualDataSize: {}, footerSize: {}, entries: {}",
                id,
                segment.name(),
                segment.physicalSize(),
                segment.logicalSize(),
                segment.actualDataSize(),
                segment.footerSize(),
                segment.entries());
    }

    private long totalLogicalSize(Log<T> log) {
        return log.headerSize() + log.uncompressedSize() + log.footerSize();
    }
}
