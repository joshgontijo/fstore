package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.compaction.combiner.ConcatenateCombiner;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;

import java.io.File;
import java.util.Objects;

public class Config<T, L extends Log<T>> {

    public final File directory;
    public final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T, L> combiner = new ConcatenateCombiner<>();
    SegmentFactory<T, L> segmentFactory = Segment::new;

    String name = "default";
    int segmentSize = (int) Size.MEGABYTE.toBytes(10);
    boolean mmap;
    boolean asyncFlush;
    int maxSegmentsPerLevel = 3;
    int mmapBufferSize = segmentSize;
    boolean flushAfterWrite;
    boolean threadPerLevel;
    boolean compactionDisabled;

    Config(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Config<T, L> segmentSize(int size) {
        this.segmentSize = size;
        return this;
    }

    public Config<T, L> name(String name) {
        this.name = name;
        return this;
    }

    public Config<T, L> maxSegmentsPerLevel(int maxSegmentsPerLevel) {
        if (maxSegmentsPerLevel <= 0) {
            throw new IllegalArgumentException("maxSegmentsPerLevel must be greater than zero");
        }
        this.maxSegmentsPerLevel = maxSegmentsPerLevel;
        return this;
    }

    public Config<T, L> disableCompaction() {
        this.compactionDisabled = true;
        return this;
    }

    public Config<T, L> threadPerLevelCompaction() {
        this.threadPerLevel = true;
        return this;
    }

    public Config<T, L> namingStrategy(NamingStrategy strategy) {
        Objects.requireNonNull(strategy, "NamingStrategy must be provided");
        this.namingStrategy = strategy;
        return this;
    }

    public Config<T, L> compactionStrategy(SegmentCombiner<T, L> combiner) {
        Objects.requireNonNull(combiner, "SegmentCombiner must be provided");
        this.combiner = combiner;
        return this;
    }

    public Config<T, L> flushAfterWrite() {
        this.flushAfterWrite = true;
        return this;
    }

    public Config<T, L> mmap() {
        this.mmap = true;
        return this;
    }

    public Config<T, L> mmap(int bufferSize) {
        this.mmap = true;
        this.mmapBufferSize = bufferSize;
        return this;
    }

    public Config<T, L> segmentFactory(SegmentFactory<T> segmentFactory) {
        Objects.requireNonNull(segmentFactory, "SegmentFactory must be provided");
        this.segmentFactory = segmentFactory;
        return this;
    }

    public Config<T, L> asyncFlush() {
        this.asyncFlush = true;
        return this;
    }

    public LogAppender<T, L> open() {
        return new LogAppender<>(this);
    }

}
