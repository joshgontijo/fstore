package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.lsmtree.sstable.Range;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.lsmtree.log.EntryAdded;
import io.joshworks.fstore.lsmtree.log.EntryDeleted;
import io.joshworks.fstore.lsmtree.log.LogRecord;
import io.joshworks.fstore.lsmtree.log.TransactionLog;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.lsmtree.sstable.SSTableCompactor;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;

    private final ExecutorService writer;

    private LsmTree(Builder<K, V> builder) {
        this.sstables = createSSTable(builder);
        this.log = createTransactionLog(builder);
        this.log.restore(this::restore);
        this.writer = new MonitoredThreadPool("lsm-write", new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, new LinkedBlockingDeque<>()));
        this.sstables.flushSync();
    }

    public static <K extends Comparable<K>, V> Builder<K, V> builder(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        return new Builder<>(directory, keySerializer, valueSerializer);
    }

    private SSTables<K, V> createSSTable(Builder<K, V> builder) {
        return new SSTables<>(
                builder.directory,
                builder.keySerializer,
                builder.valueSerializer,
                builder.name,
                builder.segmentSize,
                builder.flushThreshold,
                builder.sstableStorageMode,
                builder.ssTableFlushMode,
                builder.sstableBlockFactory,
                builder.sstableCompactor,
                builder.maxAgeSeconds,
                builder.codec,
                builder.bloomFPProb,
                builder.blockSize,
                builder.blockCache);
    }

    private TransactionLog<K, V> createTransactionLog(Builder<K, V> builder) {
        return new TransactionLog<>(
                builder.directory,
                builder.keySerializer,
                builder.valueSerializer,
                builder.name,
                builder.tlogStorageMode);
    }

    public void put(K key, V value) {
        requireNonNull(key, "Key must be provided");
        requireNonNull(value, "Value must be provided");
        final long recPos = log.append(LogRecord.add(key, value));

        Entry<K, V> entry = Entry.add(key, value);
        CompletableFuture<Void> flushTask = sstables.add(entry);
        if (flushTask != null) {
            String token = log.markFlushing();
            flushTask.thenRun(() -> log.markFlushed(token));
        }
    }

    public V get(K key) {
        Entry<K, V> entry = getEntry(key);
        return entry == null ? null : entry.value;
    }

    public Entry<K, V> getEntry(K key) {
        requireNonNull(key, "Key must be provided");
        return sstables.get(key);
    }

    public void remove(K key) {
        requireNonNull(key, "Key must be provided");
        log.append(LogRecord.delete(key));
        sstables.add(Entry.delete(key));
    }

    /**
     * Search for non deletion entries in all SSTables that matches the given {@link Predicate<Entry>}
     * SSTables are scanned from newest to oldest, including MemTable
     *
     * @param key        The key to look for
     * @param expression The expression function to apply
     * @param matcher    The matcher, used to filter entries that matches the expression
     * @return The list of found entries
     */
    public List<Entry<K, V>> findAll(K key, Expression expression, Predicate<Entry<K, V>> matcher) {
        return sstables.findAll(key, expression, matcher);
    }

    /**
     * Finds the newest, non deletion entry that matches the give {@link Predicate<Entry>}
     * SSTables are scanned from newest to oldest, including MemTable
     *
     * @param key        The key to look for
     * @param expression The expression function to apply
     * @param matcher    The matcher, used to filter entries that matches the expression
     * @return The first match, or null
     */
    public Entry<K, V> find(K key, Expression expression, Predicate<K> matcher) {
        return sstables.find(key, expression, matcher);
    }

    public long size() {
        return sstables.size();
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        return sstables.iterator(direction);
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        return sstables.iterator(direction, range);
    }

    @Override
    public void close() {
        sstables.close();
        log.close();
    }


    private void restore(LogRecord record) {
        switch (record.type) {
            case ADD:
                EntryAdded<K, V> added = (EntryAdded<K, V>) record;
                sstables.add(Entry.of(record.timestamp, added.key, added.value));
                break;
            case DELETE:
                EntryDeleted<K> deleted = (EntryDeleted<K>) record;
                sstables.add(Entry.of(record.timestamp, deleted.key, null));
                break;
        }
    }

    public void flushSync() {
        sstables.flushSync();
    }

    public CompletableFuture<Void> flush() {
        return sstables.flush();
    }

    public void compact() {
        sstables.compact();
    }

    public static class Builder<K extends Comparable<K>, V> {

        private static final int DEFAULT_THRESHOLD = 1000000;
        private static final int NO_MAX_AGE = -1;

        private final File directory;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private UniqueMergeCombiner<Entry<K, V>> sstableCompactor;
        private double bloomFPProb = 0.05;
        private int blockSize = Memory.PAGE_SIZE;
        private int flushThreshold = DEFAULT_THRESHOLD;
        private Codec codec = new SnappyCodec();
        private String name = "lsm-tree";
        private StorageMode sstableStorageMode = StorageMode.MMAP;
        private FlushMode ssTableFlushMode = FlushMode.ON_ROLL;
        private BlockFactory sstableBlockFactory = Block.vlenBlock();
        private StorageMode tlogStorageMode = StorageMode.RAF;
        private long segmentSize = Size.MB.of(32);

        private Cache<String, Block> blockCache = Cache.lruCache(1000, -1);
        private long maxAgeSeconds = NO_MAX_AGE;


        private Builder(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
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

        public Builder<K, V> bloomFilter(double bloomFPProb, long bloomNItems) {
            if (bloomFPProb <= 0) {
                throw new IllegalArgumentException("Bloom filter false positive probability must greater than zero");
            }
            if (bloomNItems <= 0) {
                throw new IllegalArgumentException("Number of expected items in the bloom filter must be greater than zero");
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

        public Builder<K, V> blockFactory(BlockFactory blockFactory) {
            this.sstableBlockFactory = blockFactory;
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

        public Builder<K, V> transacationLogStorageMode(StorageMode mode) {
            requireNonNull(mode);
            this.tlogStorageMode = mode;
            return this;
        }

        public Builder<K, V> name(String name) {
            this.name = name;
            return this;
        }

        public LsmTree<K, V> open() {
            this.sstableCompactor = this.sstableCompactor == null ? new SSTableCompactor<>(maxAgeSeconds) : sstableCompactor;
            return new LsmTree<>(this);
        }
    }

}
