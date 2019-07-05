package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.compaction.Compactor;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.log.utils.BitUtil;
import io.joshworks.fstore.log.utils.LogFileUtils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * Position address schema
 * <p>
 * |------------ 64bits -------------|
 * |---- 16 -----|------- 48 --------|
 * [SEGMENT_IDX] [POSITION_ON_SEGMENT]
 */

public class LogAppender<T> implements Closeable {

    private final Logger logger;

    private static final int SEGMENT_BITS = 16;
    private static final int SEGMENT_ADDRESS_BITS = Long.SIZE - SEGMENT_BITS;

    static final long MAX_SEGMENTS = BitUtil.maxValueForBits(SEGMENT_BITS);
    static final long MAX_SEGMENT_ADDRESS = BitUtil.maxValueForBits(SEGMENT_ADDRESS_BITS);

    private static final int FLUSH_INTERVAL_SEC = 5;

    private final File directory;
    private final Serializer<T> serializer;
    private final Metadata metadata;
    private final NamingStrategy namingStrategy;
    private final SegmentFactory<T> factory;
    private final StorageMode storageMode;
    private final BufferPool bufferPool;
    private final int maxEntrySize;
    private final double checksumProbability;
    private final int readPageSize;

    final Levels<T> levels;

    //state
    private final boolean compactionDisabled;

    private AtomicBoolean closed = new AtomicBoolean();

    private final ScheduledExecutorService flushWorker;
    private final List<ForwardLogReader<T>> forwardReaders = new CopyOnWriteArrayList<>();
    private final Compactor<T> compactor;

    public static <T> Config<T> builder(File directory, Serializer<T> serializer) {
        return new Config<>(directory, serializer);
    }

    LogAppender(Config<T> config) {
        this.directory = config.directory;
        this.serializer = config.serializer;
        this.factory = config.segmentFactory;
        this.storageMode = config.storageMode;
        this.namingStrategy = config.namingStrategy;
        this.maxEntrySize = config.maxEntrySize;
        this.checksumProbability = config.checksumProbability;
        this.readPageSize = config.bufferSize;
        this.compactionDisabled = config.compactionDisabled;
        this.bufferPool = config.bufferPool;
        this.logger = Logging.namedLogger(config.name, "appender");

        boolean metadataExists = LogFileUtils.metadataExists(directory);

        if (!metadataExists) {
            logger.info("Creating LogAppender");

            if (config.segmentSize > MAX_SEGMENT_ADDRESS) {
                throw new IllegalArgumentException("Maximum segment size allowed is " + MAX_SEGMENT_ADDRESS);
            }
            if (config.maxEntrySize > config.segmentSize) {
                throw new IllegalArgumentException("Max entry size (" + config.maxEntrySize + ") must be less or equals than segment size (" + config.segmentSize + ")");
            }

            LogFileUtils.createRoot(directory);
            this.metadata = Metadata.write(directory, config.segmentSize, config.compactionThreshold, config.flushMode);

        } else {
            logger.info("Opening LogAppender");
            this.metadata = Metadata.readFrom(directory);
        }

        if (FlushMode.PERIODICALLY.equals(metadata.flushMode)) {
            this.flushWorker = Executors.newSingleThreadScheduledExecutor();
            this.flushWorker.scheduleWithFixedDelay(this::flush, FLUSH_INTERVAL_SEC, FLUSH_INTERVAL_SEC, TimeUnit.SECONDS);
        } else {
            this.flushWorker = null;
        }

        this.levels = loadSegments();
        this.compactor = new Compactor<>(
                directory,
                config.combiner,
                factory,
                config.compactionStorage,
                serializer,
                bufferPool,
                namingStrategy,
                metadata.compactionThreshold,
                config.name,
                levels,
                config.parallelCompaction,
                maxEntrySize,
                readPageSize,
                checksumProbability);

        logConfig(config);
        if (!compactionDisabled) {
            compactor.compact();
        }
    }

    private void logConfig(Config<T> config) {
        logger.info("STORAGE LOCATION: {}", config.directory.toPath());
        logger.info("COMPACTION ENABLED: {}", !config.compactionDisabled);
        logger.info("MAX SEGMENTS: {} ({} bits)", MAX_SEGMENTS, SEGMENT_BITS);
        logger.info("MAX SEGMENT ADDRESS: {} ({} bits)", MAX_SEGMENT_ADDRESS, SEGMENT_ADDRESS_BITS);

        logger.info("BUFFER POOL: {}", config.bufferPool.getClass().getSimpleName());
        logger.info("SEGMENT SIZE: {}", config.segmentSize);
        logger.info("FLUSH MODE: {}", config.flushMode);
        logger.info("COMPACTION ENABLED: {}", !this.compactionDisabled);
        logger.info("COMPACTION THRESHOLD: {}", config.compactionThreshold);
        logger.info("STORAGE MODE: {}", config.storageMode);
    }


    private Log<T> createCurrentSegment() {
        long alignedSize = align(LogHeader.BYTES + metadata.segmentSize); //log + header
        File segmentFile = LogFileUtils.newSegmentFile(directory, namingStrategy, 1);
        return factory.createOrOpen(segmentFile, storageMode, alignedSize, serializer, bufferPool, WriteMode.LOG_HEAD, maxEntrySize, checksumProbability, readPageSize);
    }

    private static long align(long fileSize) {
        if (fileSize % Memory.PAGE_SIZE == 0) {
            return fileSize;
        }
        return Memory.PAGE_SIZE * ((fileSize / Memory.PAGE_SIZE) + 1);
    }

    private Levels<T> loadSegments() {

        List<Log<T>> segments = new ArrayList<>();
        try {
            for (String segmentName : LogFileUtils.findSegments(directory)) {
                Log<T> segment = loadSegment(segmentName);
                logger.info("Loaded segment: {}", segment);
                if (Type.MERGE_OUT.equals(segment.type()) || Type.DELETED.equals(segment.type())) {
                    logger.info("Deleting dangling segment: {}", segment);
                    segment.delete();
                    continue;
                }
                segments.add(segment);
            }

            long levelZeroSegments = segments.stream().filter(l -> l.level() == 0).count();

            if (levelZeroSegments == 0) {
                //create current segment
                Log<T> currentSegment = createCurrentSegment();
                segments.add(currentSegment);
            }
            if (levelZeroSegments > 1) {
                throw new IllegalStateException("Multiple level zero segments");
            }

            return Levels.create(segments);

        } catch (Exception e) {
            segments.forEach(IOUtils::closeQuietly);
            throw e;
        }
    }

    private Log<T> loadSegment(String segmentName) {
        File segmentFile = LogFileUtils.getSegmentHandler(directory, segmentName);
        Log<T> segment = factory.createOrOpen(segmentFile, storageMode, -1, serializer, bufferPool, null, maxEntrySize, checksumProbability, readPageSize);
        logger.info("Loaded segment {}", segment);
        return segment;
    }

    public void roll() {
        levels.lock(() -> {
            try {
                Log<T> current = levels.current();
                if (FlushMode.ON_ROLL.equals(metadata.flushMode)) {
                    current.flush();
                }
                if (current.entries() == 0) {
                    logger.warn("No entries in the current segment: {}", current.name());
                    return;
                }
                logger.info("Rolling segment: {}", current);

                current.roll(1);

                Log<T> newSegment = createCurrentSegment();
                levels.appendSegment(newSegment);

                notifyPollers(newSegment);

                if (!compactionDisabled) {
                    compactor.compact();
                }


            } catch (Exception e) {
                throw new RuntimeIOException("Could not roll segment file", e);
            }
        });
    }

    static int getSegment(long position) {
        long segmentIdx = (position >>> SEGMENT_ADDRESS_BITS);
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Invalid segment, value cannot be greater than " + MAX_SEGMENTS);
        }

        return (int) segmentIdx;
    }

    static long toSegmentedPosition(long segmentIdx, long position) {
        if (segmentIdx < 0) {
            throw new IllegalArgumentException("Segment index must be greater than zero");
        }
        if (segmentIdx > MAX_SEGMENTS) {
            throw new IllegalArgumentException("Segment index cannot be greater than " + MAX_SEGMENTS);
        }
        return (segmentIdx << SEGMENT_ADDRESS_BITS) | position;
    }

    static long getPositionOnSegment(long position) {
        long mask = (1L << SEGMENT_ADDRESS_BITS) - 1;
        return (position & mask);
    }

    private void notifyPollers(Log<T> newSegment) {
        for (ForwardLogReader<T> reader : forwardReaders) {
            reader.addSegment(newSegment);
        }
    }

    public long append(T data) {
        if (closed.get()) {
            throw new AppendException("Stored closed");
        }
        Log<T> current = levels.current();

        int segments = levels.numSegments();
        long positionOnSegment = current.append(data);
        if (positionOnSegment == Storage.EOF) {
            roll();
            return append(data);
        }
        if (FlushMode.ALWAYS.equals(metadata.flushMode)) {
            flush();
        }
        return toSegmentedPosition(segments - 1L, positionOnSegment);
    }

    public String name() {
        return directory.getName();
    }

    public LogIterator<T> iterator(Direction direction) {
        long startPosition = Direction.FORWARD.equals(direction) ? Log.START : Math.max(position(), Log.START);
        return iterator(direction, startPosition);
    }

    public Stream<T> stream(Direction direction) {
        return Iterators.closeableStream(iterator(direction));
    }

    public LogIterator<T> iterator(Direction direction, long position) {
        if (Direction.FORWARD.equals(direction)) {
            return forwardIterator(position);
        }
        return backwardIterator(position);
    }

    private LogIterator<T> backwardIterator(long position) {
        return levels.apply(Direction.BACKWARD, segments -> {
            int numSegments = segments.size();
            int segIdx = LogAppender.getSegment(position);

            segIdx = numSegments - (numSegments - segIdx);
            int skips = (numSegments - 1) - segIdx;

            LogAppender.validateSegmentIdx(segIdx, position, levels);
            long startPosition = LogAppender.getPositionOnSegment(position);
            LogIterator<Log<T>> it = Iterators.of(segments);
            // skip
            for (int i = 0; i < skips; i++) {
                it.next();
            }
            return new BackwardLogReader<>(it, startPosition, segIdx);
        });
    }

    private LogIterator<T> forwardIterator(long position) {
        return levels.apply(Direction.FORWARD, segments -> {
            int segmentIdx = LogAppender.getSegment(position);
            long startPosition = LogAppender.getPositionOnSegment(position);
            LogAppender.validateSegmentIdx(segmentIdx, startPosition, levels);
            ForwardLogReader<T> forwardLogReader = new ForwardLogReader<>(startPosition, segments, segmentIdx, this::removeReader);
            forwardReaders.add(forwardLogReader);
            return forwardLogReader;
        });
    }

    private void removeReader(ForwardLogReader reader) {
        forwardReaders.remove(reader);
    }

    public long position() {
        return toSegmentedPosition(levels.numSegments() - 1L, levels.current().position());
    }

    //must support multiple readers
    public T get(long position) {
        return levels.apply(Direction.FORWARD, segments -> {
            int segmentIdx = getSegment(position);
            validateSegmentIdx(segmentIdx, position, levels);
            long positionOnSegment = getPositionOnSegment(position);

            if (segmentIdx < 0 || segmentIdx >= segments.size()) {
                return null;
            }
            return segments.get(segmentIdx).get(positionOnSegment);
        });

    }

    static <T> void validateSegmentIdx(int segmentIdx, long pos, Levels<T> levels) {
        if (segmentIdx < 0 || segmentIdx > levels.numSegments()) {
            throw new IllegalArgumentException("No segment for address " + pos + " (segmentIdx: " + segmentIdx + "), available segments: " + levels.numSegments());
        }
    }

    public long size() {
        return levels.apply(Direction.FORWARD, segments -> segments.stream().mapToLong(Log::fileSize).sum());
    }

    public long size(int level) {
        if (level < 0) {
            throw new IllegalArgumentException("Level must be at least zero");
        }
        if (level > levels.depth()) {
            throw new IllegalArgumentException("No such level " + level + ", current depth: " + levels.depth());
        }
        return applyToSegments(Direction.FORWARD, segments -> segments.stream().mapToLong(Log::fileSize).sum());
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        logger.info("Closing log appender {}", directory.getName());

        compactor.close();
        shutdownFlushWorker();

        Log<T> currentSegment = levels.current();
        if (currentSegment != null) {
            currentSegment.flush();
        }

        closeSegments();

        for (ForwardLogReader forwardReader : forwardReaders) {
            IOUtils.closeQuietly(forwardReader);
        }
        forwardReaders.clear();
    }

    private void closeSegments() {
        levels.acquire(Direction.FORWARD, segments -> {
            try {
                for (Log<T> segment : segments) {
                    logger.info("Closing segment {}", segment.name());
                    IOUtils.closeQuietly(segment);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to close log appender", e);
            }
        });

    }

    private void shutdownFlushWorker() {
        if (flushWorker == null) {
            return;
        }
        try {
            flushWorker.shutdown();
            flushWorker.awaitTermination(FLUSH_INTERVAL_SEC * 2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Failed to close flush worker");
        }
    }

    public synchronized void flush() {
        if (closed.get()) {
            return;
        }
        levels.current().flush();
    }

    public long entries() {
        return levels.apply(Direction.FORWARD, logs -> logs.stream().mapToLong(Log::entries).sum());
    }

    public String currentSegment() {
        return levels.current().name();
    }

    public <R> R applyToSegments(Direction direction, Function<List<Log<T>>, R> function) {
        return levels.apply(direction, function);
    }

    public int depth() {
        return levels.depth();
    }

    public Path directory() {
        return directory.toPath();
    }

    Log<T> current() {
        return levels.current();
    }

    public void compact() {
        compactor.compact();
    }
}
