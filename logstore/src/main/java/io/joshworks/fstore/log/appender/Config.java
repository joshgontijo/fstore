package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.cache.CacheManager;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.compaction.combiner.ConcatenateCombiner;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;

import java.io.File;
import java.util.Objects;

public class Config<T> {

    public static final int NO_MMAP_BUFFER_SIZE = -1;
    public static final String DEFAULT_APPENDER_NAME = "default";
    public static final int DEFAULT_MAX_SEGMENT_PER_LEVEL = 3;
    public static final int DEFAULT_LOG_SIZE = (int) Size.MEGABYTE.toBytes(200);
    public static final int DEFAULT_FOOTER_SIZE = 0;

    public final File directory;
    public final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner = new ConcatenateCombiner<>();
    SegmentFactory<T> segmentFactory;

    String name = DEFAULT_APPENDER_NAME;
    int footerSize = DEFAULT_FOOTER_SIZE;
    long logSize = DEFAULT_LOG_SIZE;
    boolean mmap;
    boolean asyncFlush;
    int maxSegmentsPerLevel = DEFAULT_MAX_SEGMENT_PER_LEVEL;
    int mmapBufferSize = NO_MMAP_BUFFER_SIZE;
    boolean flushAfterWrite;
    boolean threadPerLevel;
    boolean compactionDisabled;
    long cacheSize = CacheManager.NO_CACHING;
    int cacheMaxAge = CacheManager.NO_CACHING;


    Config(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Config<T> logSize(long logSize) {
        this.logSize = logSize;
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

    public Config<T> enableCaching(long maxSize) {
        return enableCaching(maxSize, CacheManager.CACHING_NO_MAX_AGE);
    }

    public Config<T> enableCaching(long maxSize, int maxAge) {
        this.cacheSize = maxSize;
        this.cacheMaxAge = maxAge;
        return this;
    }

    public Config<T> maxSegmentsPerLevel(int maxSegmentsPerLevel) {
        if (maxSegmentsPerLevel <= 0) {
            throw new IllegalArgumentException("maxSegmentsPerLevel must be greater than zero");
        }
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
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
        Objects.requireNonNull(strategy, "NamingStrategy must be provided");
        this.namingStrategy = strategy;
        return this;
    }

    public Config<T> compactionStrategy(SegmentCombiner<T> combiner) {
        Objects.requireNonNull(combiner, "SegmentCombiner must be provided");
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

    public Config<T> mmap(int bufferSize) {
        this.mmap = true;
        this.mmapBufferSize = bufferSize;
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
        Objects.requireNonNull(segmentFactory, "SegmentFactory must be provided");
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
