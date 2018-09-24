package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.BitUtil;
import io.joshworks.fstore.log.appender.compaction.combiner.ConcatenateCombiner;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;

import java.io.File;
import java.util.Objects;

public class Config<T> {

    //reader / writer max message size
    public static final int MAX_RECORD_SIZE = 131072;

    //How many bits a segment index can hold
    private static final int SEGMENT_BITS = 18;

    public final File directory;
    public final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner = new ConcatenateCombiner<>();

    int segmentBitShift = Long.SIZE - SEGMENT_BITS;
    int segmentSize = (int) Size.MEGABYTE.toBytes(10);
    boolean mmap;
    boolean asyncFlush;
    int maxSegmentsPerLevel = 3;
    int mmapBufferSize = segmentSize;
    int maxRecordSize = MAX_RECORD_SIZE;
    double checksumProb = 1;
    boolean flushAfterWrite;
    boolean threadPerLevel;
    boolean compactionDisabled;
    boolean directBuffers;
    int numBuffers;

    Config(File directory, Serializer<T> serializer) {
        Objects.requireNonNull(directory, "directory cannot be null");
        Objects.requireNonNull(serializer, "serializer cannot be null");
        this.directory = directory;
        this.serializer = serializer;
    }

    public Config<T> segmentSize(int size) {
        long maxAddress = BitUtil.maxValueForBits(segmentBitShift);
        if (size > maxAddress) {
            throw new IllegalArgumentException("Maximum position allowed is " + maxAddress);
        }
        this.segmentSize = size;
        return this;
    }

    public Config<T> maxSegmentsPerLevel(int maxSegmentsPerLevel) {
        if(maxSegmentsPerLevel <= 0) {
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

    public Config<T> maxRecordSize(int maxRecordSize) {
        this.maxRecordSize = maxRecordSize;
        return this;
    }

    public Config<T> numBuffers(int numBuffers) {
        this.numBuffers = numBuffers;
        return this;
    }

    public Config<T> checksumProbability(double checksumProb) {
        if (checksumProb < 0 || checksumProb > 1) {
            throw new IllegalArgumentException("Checksum verification frequency must be between 0.0 and 1.0");
        }
        this.checksumProb = checksumProb;
        return this;
    }

    public Config<T> useDirectBuffers(boolean directBuffers) {
        this.directBuffers = directBuffers;
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

}
