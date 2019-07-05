package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.header.LogHeader;
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

            long newSegmentLogSize = segments.stream().mapToLong(Log::uncompressedSize).sum();
            long logSize = newSegmentLogSize + LogHeader.BYTES;

            String names = Arrays.toString(segments.stream().map(Log::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment logSize: {} fileSize: {}", names, level, combiner.getClass().getSimpleName(), newSegmentLogSize, logSize);

            for (int i = 0; i < segments.size(); i++) {
                Log<T> segment = segments.get(i);
                logger.info("Segment[{}] {} - size: {}, entries: {}", i, segment.name(), segment.position(), segment.entries());
            }

            long start = System.currentTimeMillis();

            output = segmentFactory.createOrOpen(segmentFile, storageMode, logSize, serializer, bufferPool, WriteMode.MERGE_OUT, checksumProbability, readPageSize);

            combiner.merge(segments, output);
            output.flush();

            logger.info("Result Segment {} - final size: {}, entries: {}", output.name(), output.fileSize(), output.entries());

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            onComplete.accept(CompactionResult.success(segments, output, level));

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            onComplete.accept(CompactionResult.failure(segments, output, level, e));
        }
    }
}
