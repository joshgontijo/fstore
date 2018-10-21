package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
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

    public static final String DEFAULT_APPENDER_NAME = "default";
    public static final int COMPACTION_THRESHOLD = 3;
    public static final long DEFAULT_LOG_SIZE = Size.MB.of(200);
    public static final int DEFAULT_FOOTER_SIZE = 0;
    public static final double DEFAULT_CHECKSUM_PROB = 1.0;

    public final File directory;
    public final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner = new ConcatenateCombiner<>();
    SegmentFactory<T> segmentFactory;
    BufferPool bufferPool = new SingleBufferThreadCachedPool(false);

    String name = DEFAULT_APPENDER_NAME;
    int footerSize = DEFAULT_FOOTER_SIZE;
    long logSize = DEFAULT_LOG_SIZE;
    double checksumProbability = DEFAULT_CHECKSUM_PROB;
    boolean mmap;
    boolean asyncFlush;
    int compactionThreshold = COMPACTION_THRESHOLD;
    boolean flushAfterWrite;
    boolean threadPerLevel;
    boolean compactionDisabled;
    boolean rafCache;

    Config(File directory, Serializer<T> serializer) {
        this.directory = requireNonNull(directory, "directory cannot be null");;
        this.serializer = requireNonNull(serializer, "serializer cannot be null");;
    }

    public Config<T> segmentSize(long segmentSize) {
        this.logSize = segmentSize;
        return this;
    }

    public Config<T> checksumProbability(double checksumProbability) {
        if(checksumProbability < 0 || checksumProbability > 1) {
            throw new IllegalStateException("Checksum probability must be between 0 and 1");
        }
        this.checksumProbability = checksumProbability;
        return this;
    }

    public Config<T> bufferPool(BufferPool bufferPool) {
        this.bufferPool = requireNonNull(bufferPool, "BufferPool cannot be null");
        return this;
    }

    public Config<T> footerSize(int footerSize) {
        this.footerSize = footerSize;
        return this;
    }

    public Config<T> name(String name) {
        this.name = name;
        return this;
    }

    public Config<T> enableCaching() {
        this.rafCache = true;
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

    public Config<T> threadPerLevelCompaction() {
        this.threadPerLevel = true;
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

    public Config<T> flushAfterWrite() {
        this.flushAfterWrite = true;
        return this;
    }

    public Config<T> mmap() {
        this.mmap = true;
        return this;
    }

    public Config<T> asyncFlush() {
        this.asyncFlush = true;
        return this;
    }

    public LogAppender<T> open() {
        return open(Segment::new);
    }

    public LogAppender<T> open(SegmentFactory<T> segmentFactory) {
        requireNonNull(segmentFactory, "SegmentFactory must be provided");
        this.segmentFactory = segmentFactory;
        return new LogAppender<>(this);
    }

//    public LogAppender<T> openBlockAppender() {
//        return openBlockAppender(VLenBlock.factory());
//    }
//
//    public LogAppender<T> openBlockAppender(BlockFactory<T> blockFactory) {
//        return openBlockAppender(blockFactory, new SnappyCodec(), Memory.PAGE_SIZE);
//    }
//
//    public LogAppender<T> openBlockAppender(BlockFactory<T> blockFactory, int maxBlockSize) {
//        return openBlockAppender(blockFactory, new SnappyCodec(), maxBlockSize);
//    }
//
//    public LogAppender<T> openBlockAppender(BlockFactory<T> blockFactory, Codec codec, int maxBlockSize) {
//        Objects.requireNonNull(blockFactory, "BlockFactory must be provided");
//        Objects.requireNonNull(codec, "Codec must be provided");
//        if (maxBlockSize < Memory.PAGE_SIZE) {
//            throw new IllegalArgumentException("Block must be at least " + Memory.PAGE_SIZE);
//        }
//        this.blockSize = maxBlockSize;
//        return openBlockAppender(new BlockSegmentFactory<>(blockFactory, codec, maxBlockSize), maxBlockSize);
//    }
//
//    public LogAppender<T> openBlockAppender(SegmentFactory<T> segmentFactory) {
//        return openBlockAppender(segmentFactory, Memory.PAGE_SIZE);
//    }
//
//    public LogAppender<T> openBlockAppender(SegmentFactory<T> segmentFactory, int maxBlockSize) {
//        this.segmentFactory = segmentFactory;
//        this.blockSize = maxBlockSize;
//        return new LogAppender<>(this);
//    }

}
