package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.seda.EventContext;
import io.joshworks.fstore.core.seda.SedaContext;
import io.joshworks.fstore.core.seda.Stage;
import io.joshworks.fstore.log.LogFileUtils;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.utils.Logging;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Compactor<T> {

    private final Logger logger;

    static final String COMPACTION_CLEANUP_STAGE = "compaction-cleanup";
    static final String COMPACTION_MANAGER = "compaction-manager";

    private File directory;
    private final SegmentCombiner<T> segmentCombiner;

    private final SegmentFactory<T> segmentFactory;
    private final StorageProvider storageProvider;
    private Serializer<T> serializer;
    private IDataStream dataStream;
    private NamingStrategy namingStrategy;
    private final int compactionThreshold;
    private final String magic;
    private final String name;
    private Levels<T> levels;
    private final SedaContext sedaContext;
    private final boolean threadPerLevel;

    private final Set<Log<T>> compacting = new HashSet<>();

    private final CompactionTask<T> compactionTask = new CompactionTask<>();

    public Compactor(File directory,
                     SegmentCombiner<T> segmentCombiner,
                     SegmentFactory<T> segmentFactory,
                     StorageProvider storageProvider,
                     Serializer<T> serializer,
                     IDataStream dataStream,
                     NamingStrategy namingStrategy,
                     int compactionThreshold,
                     String magic,
                     String name,
                     Levels<T> levels,
                     SedaContext sedaContext,
                     boolean threadPerLevel) {
        this.directory = directory;
        this.segmentCombiner = segmentCombiner;
        this.segmentFactory = segmentFactory;
        this.storageProvider = storageProvider;
        this.serializer = serializer;
        this.dataStream = dataStream;
        this.namingStrategy = namingStrategy;
        this.compactionThreshold = compactionThreshold;
        this.magic = magic;
        this.name = name;
        this.levels = levels;
        this.sedaContext = sedaContext;
        this.threadPerLevel = threadPerLevel;
        this.logger = Logging.namedLogger(name, "compactor");

        sedaContext.addStage(COMPACTION_CLEANUP_STAGE, this::cleanup, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
        sedaContext.addStage(COMPACTION_MANAGER, this::compact, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
    }

    public void forceCompaction(int level) {
        if (level <= 0) {
            throw new IllegalArgumentException("Level must be greater than 1");
        }
        sedaContext.submit(COMPACTION_MANAGER, new CompactionRequest(level, true));
    }

    public void requestCompaction(int level) {
        if (level <= 0) {
            throw new IllegalArgumentException("Level must be greater than 1");
        }
        sedaContext.submit(COMPACTION_MANAGER, new CompactionRequest(level, false));
    }

    private synchronized void compact(EventContext<CompactionRequest> event) {
        int level = event.data.level;
        boolean force = event.data.force;

        synchronized (this) {
            if (!requiresCompaction(level) && !force) {
                return;
            }
            List<Log<T>> segmentsForCompaction = segmentsForCompaction(level);
            if (segmentsForCompaction.isEmpty()) {
                logger.warn("Level {} is empty, nothing to compact", level);
                return;
            }
            if (segmentsForCompaction.size() < compactionThreshold) {
                logger.warn("Number of segments below threshold on level {}", level);
                return;
            }
            compacting.addAll(segmentsForCompaction);

            logger.info("Compacting level {}", level);

            File targetFile = LogFileUtils.newSegmentFile(directory, namingStrategy, level + 1);

            String stageName = stageName(level);
            if (!sedaContext.stages().contains(stageName)) {
                sedaContext.addStage(stageName, compactionTask, new Stage.Builder().corePoolSize(1).maximumPoolSize(1));
            }
            event.submit(stageName, new CompactionEvent<>(segmentsForCompaction, segmentCombiner, targetFile, segmentFactory, storageProvider, serializer, dataStream, name, level, magic));
        }
    }

    private String stageName(int level) {
        return threadPerLevel ? "compaction-level-" + level : "compaction";
    }

    private synchronized void cleanup(EventContext<CompactionResult<T>> context) {
        CompactionResult<T> result = context.data;
        Log<T> target = result.target;
        int level = result.level;
        List<Log<T>> sources = result.sources;

        if (!result.successful()) {
            //TODO
            logger.error("Compaction error", result.exception);
            logger.info("Deleting failed merge result segment");
            target.delete();
            return;
        }

        if (target.entries() == 0) {
            logger.info("No entries were found in the result segment {}, deleting", target.name());
            deleteAll(List.of(target));
            levels.remove(sources);
        } else {
            levels.merge(sources, target);
        }

        compacting.removeAll(sources);

        context.submit(COMPACTION_MANAGER, new CompactionRequest(level, false));
        context.submit(COMPACTION_MANAGER, new CompactionRequest(level + 1, false));

        deleteAll(sources);
    }

    private synchronized boolean requiresCompaction(int level) {
        if (compactionThreshold <= 0 || level < 0) {
            return false;
        }

        long compactingForLevel = new ArrayList<>(compacting).stream().filter(l -> l.level() == level).count();
        int levelSize = levels.segments(level).size();
        return levelSize - compactingForLevel >= compactionThreshold;
    }

    private synchronized List<Log<T>> segmentsForCompaction(int level) {
        List<Log<T>> toBeCompacted = new ArrayList<>();
        if (level <= 0) {
            throw new IllegalArgumentException("Level must be greater than zero");
        }
        for (Log<T> segment : levels.segments(level)) {
            if (!compacting.contains(segment)) {
                toBeCompacted.add(segment);
            }
            if (toBeCompacted.size() >= compactionThreshold) {
                break;
            }
        }
        return toBeCompacted;
    }

    //delete all source segments only if all of them are not being used
    private void deleteAll(List<Log<T>> segments) {
        for (Log<T> segment : segments) {
            String segmentName = segment.name();
            try {
                segment.delete();
            } catch (Exception e) {
                logger.error("Failed to delete " + segmentName, e);
            }
        }
    }

    private static class CompactionRequest {
        private final int level;
        private final boolean force;

        private CompactionRequest(int level, boolean force) {
            this.level = level;
            this.force = force;
        }
    }

}
