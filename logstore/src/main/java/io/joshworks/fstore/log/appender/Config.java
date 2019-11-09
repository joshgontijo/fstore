package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.appender.naming.ShortUUIDNamingStrategy;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;

import java.io.File;

import static java.util.Objects.requireNonNull;

public class Config<T> {


    final File directory;
    final Serializer<T> serializer;
    NamingStrategy namingStrategy = new ShortUUIDNamingStrategy();
    SegmentCombiner<T> combiner;
    SegmentFactory<T> segmentFactory;

    String name = "default";
    long segmentSize = Size.MB.of(256);
    double checksumProbability = 1.0;
    StorageMode storageMode = StorageMode.RAF;
    FlushMode flushMode = FlushMode.MANUAL;
    int compactionThreshold = 3;
    boolean parallelCompaction;
    int readPageSize = Memory.PAGE_SIZE;
    boolean directBufferPool = false;
    StorageMode compactionStorage;
    int maxEntrySize = Size.MB.ofInt(2);

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

    public Config<T> readPageSize(int readPageSize) {
        if (this.readPageSize < 0) {
            throw new IllegalArgumentException("bufferSize must be greater than zero");
        }
        this.readPageSize = readPageSize;
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

    public Config<T> directBufferPool() {
        this.directBufferPool = true;
        return this;
    }

    public Config<T> maxEntrySize(int maxEntrySize) {
        this.maxEntrySize = maxEntrySize;
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

    public Config<T> parallelCompaction() {
        this.parallelCompaction = true;
        return this;
    }

    public Config<T> namingStrategy(NamingStrategy strategy) {
        this.namingStrategy = requireNonNull(strategy, "NamingStrategy must be provided");;
        return this;
    }

    public Config<T> compactionStrategy(SegmentCombiner<T> combiner) {
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

    @Override
    public String toString() {
        String combinerName = combiner == null ? "(none)" : combiner.getClass().getSimpleName();
        return "Config{" + "directory=" + directory +
                ", serializer=" + serializer.getClass().getSimpleName() +
                ", namingStrategy=" + namingStrategy.getClass().getSimpleName() +
                ", combiner=" + combinerName +
                ", segmentFactory=" + segmentFactory.getClass().getSimpleName() +
                ", name='" + name + '\'' +
                ", segmentSize=" + segmentSize +
                ", checksumProbability=" + checksumProbability +
                ", storageMode=" + storageMode +
                ", flushMode=" + flushMode +
                ", compactionThreshold=" + compactionThreshold +
                ", parallelCompaction=" + parallelCompaction +
                ", readPageSize=" + readPageSize +
                ", directBufferPool=" + directBufferPool +
                ", compactionStorage=" + compactionStorage +
                ", maxEntrySize=" + maxEntrySize +
                '}';
    }
}
