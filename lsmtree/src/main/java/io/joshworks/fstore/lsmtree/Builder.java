package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.compaction.combiner.MergeCombiner;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;
import io.joshworks.fstore.lsmtree.sstable.SSTableCompactor;

import java.io.File;

import static io.joshworks.fstore.lsmtree.sstable.entry.Entry.NO_MAX_AGE;
import static java.util.Objects.requireNonNull;

public class Builder<K extends Comparable<K>, V> {


    final File directory;
    final Serializer<K> keySerializer;
    final Serializer<V> valueSerializer;
    MergeCombiner<Entry<K, V>> sstableCompactor;
    int flushThreshold = 1000000;
    Codec codec = new SnappyCodec();
    String name = "lsm-tree";
    StorageMode sstableStorageMode = StorageMode.MMAP;
    FlushMode ssTableFlushMode = FlushMode.ON_ROLL;
    StorageMode tlogStorageMode = StorageMode.RAF;

    long tlogSize = Size.MB.of(256);
    int tlogCompactionThreshold = 3;
    int sstableCompactionThreshold = 3;
    int blockSize = Size.KB.ofInt(4);
    long segmentSize = Size.MB.of(12);
    int maxEntrySize = Size.MB.ofInt(2);
    double bloomFPProb = 0.05;
    int flushQueueSize = 3;
    boolean parallelCompaction;
    boolean directBufferPool;

    Cache<String, Block> blockCache = Cache.lruCache(1000, -1);
    long maxAgeSeconds = NO_MAX_AGE;


    Builder(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.directory = directory;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    public Builder<K, V> flushThreshold(int flushThreshold) {
        if (flushThreshold <= 0) {
            throw new IllegalArgumentException("Flush threshold must greater than zero");
        }
        this.flushThreshold = flushThreshold;
        return this;
    }

    public Builder<K, V> parallelCompaction(boolean parallelCompaction) {
        this.parallelCompaction = parallelCompaction;
        return this;
    }

    public Builder<K, V> useDirectBufferPool(boolean directBufferPool) {
        this.directBufferPool = directBufferPool;
        return this;
    }

    public Builder<K, V> flushQueueSize(int flushQueueSize) {
        if (flushQueueSize < 1) {
            throw new IllegalArgumentException("Flush threshold must greater than zero");
        }
        this.flushQueueSize = flushQueueSize;
        return this;
    }

    public Builder<K, V> bloomFilterFalsePositiveProbability(double bloomFPProb) {
        if (bloomFPProb <= 0) {
            throw new IllegalArgumentException("Bloom filter false positive probability must greater than zero");
        }
        this.bloomFPProb = bloomFPProb;
        return this;
    }

    public Builder<K, V> codec(Codec codec) {
        requireNonNull(codec, "Codec cannot be null");
        this.codec = codec;
        return this;
    }

    public Builder<K, V> maxAge(long maxAgeSeconds) {
        this.maxAgeSeconds = maxAgeSeconds;
        return this;
    }

    public Builder<K, V> sstableCompactor(UniqueMergeCombiner<Entry<K, V>> sstableCompactor) {
        this.sstableCompactor = sstableCompactor;
        return this;
    }

    public Builder<K, V> segmentSize(long size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Segment size must be greater than zero");
        }
        this.segmentSize = size;
        return this;
    }

    public Builder<K, V> maxEntrySize(int maxEntrySize) {
        if (maxEntrySize <= 0) {
            throw new IllegalArgumentException("Segment size must be greater than zero");
        }
        this.maxEntrySize = maxEntrySize;
        return this;
    }

    public Builder<K, V> blockSize(int blockSize) {
        this.blockSize = blockSize;
        return this;
    }

    public Builder<K, V> blockCache(Cache<String, Block> blockCache) {
        this.blockCache = requireNonNull(blockCache, "Cache must be provided");
        return this;
    }

    public Builder<K, V> sstableStorageMode(StorageMode mode) {
        requireNonNull(mode, "StorageMode cannot be null");
        requireNonNull(mode);
        this.sstableStorageMode = mode;
        return this;
    }

    public Builder<K, V> ssTableFlushMode(FlushMode mode) {
        requireNonNull(mode);
        this.ssTableFlushMode = mode;
        return this;
    }

    public Builder<K, V> transactionLogStorageMode(StorageMode mode) {
        requireNonNull(mode);
        this.tlogStorageMode = mode;
        return this;
    }

    public Builder<K, V> transactionLogSize(long tlogSize) {
        this.tlogSize = tlogSize;
        return this;
    }

    public Builder<K, V> transactionLogCompactionThreshold(int compactionThreshold) {
        this.tlogCompactionThreshold = compactionThreshold;
        return this;
    }

    public Builder<K, V> sstableCompactionThreshold(int compactionThreshold) {
        this.sstableCompactionThreshold = compactionThreshold;
        return this;
    }

    public Builder<K, V> name(String name) {
        this.name = name;
        return this;
    }

    public LsmTree<K, V> open() {
        if (maxEntrySize > segmentSize) {
            throw new IllegalStateException("Segment size must be greater than max entry size");
        }
        if (blockSize > segmentSize) {
            throw new IllegalStateException("Segment size must be greater than block size");
        }
        this.sstableCompactor = this.sstableCompactor == null ? new SSTableCompactor<>(maxAgeSeconds) : sstableCompactor;
        return new LsmTree<>(this);
    }
}
