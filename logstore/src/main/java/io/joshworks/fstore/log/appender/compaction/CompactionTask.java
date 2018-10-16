package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.seda.EventContext;
import io.joshworks.fstore.core.seda.StageHandler;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.appender.StorageProvider;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.utils.Logging;
import org.slf4j.Logger;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.joshworks.fstore.log.appender.compaction.Compactor.COMPACTION_CLEANUP_STAGE;

public class CompactionTask<T> implements StageHandler<CompactionEvent<T>> {


    @Override
    public void onEvent(EventContext<CompactionEvent<T>> context) {

        CompactionEvent<T> data = context.data;

        String magic = data.magic;
        int level = data.level;
        String name = data.name;
        int nextLevel = data.level + 1;
        File segmentFile = data.segmentFile;
        SegmentCombiner<T> combiner = data.combiner;
        List<Log<T>> segments = data.segments;
        IDataStream dataStream = data.dataStream;
        Serializer<T> serializer = data.serializer;
        StorageProvider storageProvider = data.storageProvider;
        SegmentFactory<T> segmentFactory = data.segmentFactory;

        final Logger logger = Logging.namedLogger(name, "compaction-task-" + level);

        Log<T> target = null;
        try {

            long totalSize = segments.stream().mapToLong(Log::actualSize).sum();

            String names = Arrays.toString(segments.stream().map(Log::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment size: {}", names, level, combiner.getClass().getSimpleName(), totalSize);

            for (int i = 0; i < segments.size(); i++) {
                Log<T> segment = segments.get(i);
                logger.info("Segment[{}] {} - size: {}, entries: {}", i, segment.name(), segment.fileSize(), segment.entries());
            }

            long start = System.currentTimeMillis();

            Storage storage = storageProvider.create(segmentFile, totalSize);
            target = segmentFactory.createOrOpen(storage, serializer, dataStream, magic, Type.MERGE_OUT);

            combiner.merge(segments, target);

            target.roll(nextLevel);

            logger.info("Result Segment {} - size: {}, entries: {}", target.name(), totalSize, target.entries());

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            context.submit(COMPACTION_CLEANUP_STAGE, CompactionResult.success(segments, target, level));

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            context.submit(COMPACTION_CLEANUP_STAGE, CompactionResult.failure(segments, target, level, e));
        }
    }
}
