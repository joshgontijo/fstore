package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.compaction.combiner.ConcatenateCombiner;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;

import java.io.File;

import static java.util.Objects.requireNonNull;

public class Config<T> {

    private static final String DEFAULT_APPENDER_NAME = "default";
    private static final int COMPACTION_THRESHOLD = 3;
    private static final long DEFAULT_SEGMENT_SIZE = Size.MB.of(256);
    private static final double DEFAULT_CHECKSUM_PROB = 1.0;
    private static final int DEFAULT_BUFFER_SIZE = Size.KB.intOf(4);

    final File directory;
    final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner = new ConcatenateCombiner<>();
    SegmentFactory<T> segmentFactory;
    BufferPool bufferPool = new BufferPool(false);

    String name = DEFAULT_APPENDER_NAME;
    long segmentSize = DEFAULT_SEGMENT_SIZE;
    double checksumProbability = DEFAULT_CHECKSUM_PROB;
    StorageMode storageMode = StorageMode.RAF;
    FlushMode flushMode = FlushMode.MANUAL;
    int compactionThreshold = COMPACTION_THRESHOLD;
    boolean parallelCompaction;
    boolean compactionDisabled;
    int bufferSize = DEFAULT_BUFFER_SIZE;
    StorageMode compactionStorage;

    Config(File directory, Serializer<T> serializer) {
        this.directory = requireNonNull(directory, "directory cannot be null");
        this.serializer = requireNonNull(serializer, "serializer cannot be null");
    }

    public Config<T> segmentSize(long segmentSize) {
        this.segmentSize = segmentSize;
        return this;
    }

    public Config<T> storageMode(StorageMode mode) {
        this.storageMode = requireNonNull(mode);
        return this;
    }

    public Config<T> bufferSize(int bufferSize) {
        if (bufferSize < 0) {
            throw new IllegalArgumentException("bufferSize must be greater than zero");
        }
        this.bufferSize = bufferSize;
        return this;
    }

    public Config<T> compactionStorageMode(StorageMode mode) {
        this.compactionStorage = requireNonNull(mode);
        return this;
    }

    public Config<T> checksumProbability(double checksumProbability) {
        if (checksumProbability < 0 || checksumProbability > 1) {
            throw new IllegalStateException("Checksum probability must be between 0 and 1");
        }
        this.checksumProbability = checksumProbability;
        return this;
    }

    public Config<T> bufferPool(BufferPool bufferPool) {
        this.bufferPool = requireNonNull(bufferPool, "BufferPool cannot be null");
        return this;
    }

    public Config<T> name(String name) {
        this.name = name;
        return this;
    }

    public Config<T> compactionThreshold(int compactionThreshold) {
        if (compactionThreshold <= 0) {
            throw new IllegalArgumentException("compactionThreshold must be greater than zero");
        }
        this.compactionThreshold = compactionThreshold;
        return this;
    }

    public Config<T> disableCompaction() {
        this.compactionDisabled = true;
        return this;
    }

    public Config<T> enableParallelCompaction() {
        this.parallelCompaction = true;
        return this;
    }

    public Config<T> namingStrategy(NamingStrategy strategy) {
        requireNonNull(strategy, "NamingStrategy must be provided");
        this.namingStrategy = strategy;
        return this;
    }

    public Config<T> compactionStrategy(SegmentCombiner<T> combiner) {
        requireNonNull(combiner, "SegmentCombiner must be provided");
        this.combiner = combiner;
        return this;
    }

    public Config<T> flushMode(FlushMode mode) {
        this.flushMode = requireNonNull(mode);
        return this;
    }

    public LogAppender<T> open() {
        return open(Segment::new);
    }

    public LogAppender<T> open(SegmentFactory<T> segmentFactory) {
        requireNonNull(segmentFactory, "SegmentFactory must be provided");
        this.segmentFactory = segmentFactory;
        this.compactionStorage = this.compactionStorage == null ? storageMode : compactionStorage;
        return new LogAppender<>(this);
    }

}
