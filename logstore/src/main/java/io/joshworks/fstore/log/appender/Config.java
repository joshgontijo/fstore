package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.compaction.combiner.ConcatenateCombiner;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSegmentFactory;
import io.joshworks.fstore.log.segment.block.VLenBlock;

import java.io.File;
import java.util.Objects;

public class Config<T> {

    //How many bits a segment index can hold


    public final File directory;
    public final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner = new ConcatenateCombiner<>();
    SegmentFactory<T> segmentFactory;

    int segmentSize = (int) Size.MEGABYTE.toBytes(10);
    boolean mmap;
    boolean asyncFlush;
    int maxSegmentsPerLevel = 3;
    int mmapBufferSize = segmentSize;
    boolean flushAfterWrite;
    boolean threadPerLevel;
    boolean compactionDisabled;

    //block
    int blockSize = 0;

    Config(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Config<T> segmentSize(int size) {
        this.segmentSize = size;
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

    public Config<T> segmentFactory(SegmentFactory<T> factory) {
        Objects.requireNonNull(factory, "SegmentFactory must be provided");
        this.segmentFactory = factory;
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

    public LogAppender<T> openBlockAppender() {
       return openBlockAppender(VLenBlock.factory());
    }

    public LogAppender<T> openBlockAppender(BlockFactory<T> blockFactory) {
        return openBlockAppender(blockFactory, new SnappyCodec(), Memory.PAGE_SIZE);
    }

    public LogAppender<T> openBlockAppender(BlockFactory<T> blockFactory, int maxBlockSize) {
        return openBlockAppender(blockFactory, new SnappyCodec(), maxBlockSize);
    }

    public LogAppender<T> openBlockAppender(BlockFactory<T> blockFactory, Codec codec, int maxBlockSize) {
        Objects.requireNonNull(blockFactory, "BlockFactory must be provided");
        Objects.requireNonNull(codec, "Codec must be provided");
        if (maxBlockSize < Memory.PAGE_SIZE) {
            throw new IllegalArgumentException("Block must be at least " + Memory.PAGE_SIZE);
        }
        this.blockSize = maxBlockSize;
        this.segmentFactory = new BlockSegmentFactory<>(blockFactory, codec, maxBlockSize);
        return new LogAppender<>(this);
    }

}
