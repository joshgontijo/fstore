package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.cache.Cache;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.lsmtree.log.NoOpTransactionLog;
import io.joshworks.fstore.lsmtree.log.PersistentTransactionLog;
import io.joshworks.fstore.lsmtree.log.Record;
import io.joshworks.fstore.lsmtree.log.TransactionLog;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.MemTable;
import io.joshworks.fstore.lsmtree.sstable.SSTable;
import io.joshworks.fstore.lsmtree.sstable.SSTables;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;
    private final int flushThreshold;
    private final boolean logDisabled;

    private MemTable<K, V> memTable;
    private final Cache<K, V> cache;

    private LsmTree(Builder<K, V> builder) {
        this.sstables = createSSTable(builder);
        this.log = createTransactionLog(builder);
        this.memTable = new MemTable<>();
        this.flushThreshold = builder.flushThreshold;
        this.logDisabled = builder.logDisabled;
        this.log.restore(this::restore);
        this.cache = Cache.create(builder.entryCacheSize, builder.entryCacheMaxAge);
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
                builder.sstableStorageMode,
                builder.ssTableFlushMode,
                builder.sstableBlockFactory,
                builder.codec,
                builder.bloomNItems,
                builder.bloomFPProb,
                builder.blockSize,
                builder.blockCacheSize,
                builder.blockCacheMaxAge);
    }

    private TransactionLog<K, V> createTransactionLog(Builder<K, V> builder) {
        if (builder.logDisabled) {
            return new NoOpTransactionLog<>();
        }

        return new PersistentTransactionLog<>(
                builder.directory,
                builder.keySerializer,
                builder.valueSerializer,
                builder.name,
                builder.tlogStorageMode);
    }

    //returns true if the index was flushed
    public boolean put(K key, V value) {
        log.append(Record.add(key, value));
        memTable.add(key, value);
        cache.remove(key); //evict
        if (memTable.size() >= flushThreshold) {
            flushMemTable(false);
            return true;
        }
        return false;
    }

    public V get(K key) {
        V cached = cache.get(key);
        if (cached != null) {
            return cached;
        }
        V found = memTable.get(key);
        if (found != null) {
            cache.add(key, found);
            return found;
        }
        return sstables.get(key);
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
        List<Entry<K, V>> found = new ArrayList<>();
        Entry<K, V> memEntry = expression.apply(key, memTable);
        if (memEntry != null && matcher.test(memEntry)) {
            found.add(memEntry);
        }
        List<Entry<K, V>> fromDisk = sstables.applyToSegments(Direction.BACKWARD, segments -> {
            List<Entry<K, V>> diskEntries = new ArrayList<>();
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> diskEntry = expression.apply(key, sstable);
                if (diskEntry != null && matcher.test(diskEntry)) {
                    diskEntries.add(diskEntry);
                }
            }
            return diskEntries;
        });
        found.addAll(fromDisk);
        return found;
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
    public Entry<K, V> find(K key, Expression expression, Predicate<Entry<K, V>> matcher) {
        Entry<K, V> fromMem = expression.apply(key, memTable);
        if (matchEntry(matcher, fromMem)) {
            return fromMem;
        }
        return sstables.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (!segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> fromDisk = expression.apply(key, sstable);
                if (matchEntry(matcher, fromDisk)) {
                    return fromDisk;
                }
            }
            return null;
        });
    }

    private boolean matchEntry(Predicate<Entry<K, V>> matcher, Entry<K, V> entry) {
        return entry != null && !entry.deletion && matcher.test(entry);
    }

    public boolean remove(K key) {
        if (memTable.delete(key)) {
            return true;
        }

        V found = get(key);
        if (found == null) {
            return false;
        }
        log.append(Record.delete(key));
        return true;
    }

    public long size() {
        return sstables.size();
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        List<LogIterator<Entry<K, V>>> segmentsIterators = sstables.segmentsIterator(direction);
        return new LsmTreeIterator<>(segmentsIterators, memTable.iterator());
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        List<LogIterator<Entry<K, V>>> segmentsIterators = sstables.segmentsIterator(direction, range);
        return new LsmTreeIterator<>(segmentsIterators, memTable.iterator());
    }

    @Override
    public void close() {
        if (logDisabled && !memTable.isEmpty()) {
            flushMemTable(true);
        }
        sstables.close();
        log.close();
    }

    public synchronized void flushMemTable(boolean force) {
        if (!force && memTable.size() < flushThreshold) {
            return;
        }
        MemTable<K, V> tmp = memTable;
        tmp.writeTo(sstables);
        memTable = new MemTable<>();
        log.markFlushed();
    }

    private void restore(Record<K, V> record) {
        if (EntryType.ADD.equals(record.type)) {
            memTable.add(record.key, record.value);
        }
        if (EntryType.DELETE.equals(record.type)) {
            memTable.delete(record.key);
        }
    }

    public void compact() {
        sstables.compact();
    }

    public static class Builder<K extends Comparable<K>, V> {

        private static final int DEFAULT_THRESHOLD = 1000000;

        private final File directory;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private long bloomNItems = DEFAULT_THRESHOLD;
        private double bloomFPProb = 0.05;
        private int blockSize = Memory.PAGE_SIZE;
        private int flushThreshold = DEFAULT_THRESHOLD;
        private boolean logDisabled;
        public Codec codec = new SnappyCodec();
        private String name = "lsm-tree";
        private StorageMode sstableStorageMode = StorageMode.MMAP;
        private FlushMode ssTableFlushMode = FlushMode.ON_ROLL;
        private BlockFactory sstableBlockFactory = VLenBlock.factory();
        private StorageMode tlogStorageMode = StorageMode.RAF;
        private int segmentSize = Size.MB.ofInt(32);

        private int blockCacheSize = 500;
        private int blockCacheMaxAge = 120000;

        private int entryCacheSize = 10000;
        private int entryCacheMaxAge = 120000;


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
            this.bloomNItems = bloomNItems;
            return this;
        }

        public Builder<K, V> codec(Codec codec) {
            requireNonNull(codec, "Codec cannot be null");
            this.codec = codec;
            return this;
        }

        public Builder<K, V> segmentSize(int size) {
            if (bloomNItems <= 0) {
                throw new IllegalArgumentException("Segment size must be greater than zero");
            }
            this.segmentSize = size;
            return this;
        }

        public Builder<K, V> offHeapBlock() {
            this.sstableBlockFactory = VLenBlock.factory(true);
            return this;
        }

        public Builder<K, V> blockCache(int cacheSize, int maxAgeSeconds) {
            this.blockCacheSize = cacheSize;
            this.blockCacheMaxAge = maxAgeSeconds * 1000;
            return this;
        }

        public Builder<K, V> entryCache(int cacheSize, int maxAgeSeconds) {
            this.entryCacheSize = cacheSize;
            this.entryCacheMaxAge = maxAgeSeconds * 1000;
            return this;
        }

        public Builder<K, V> disableTransactionLog() {
            this.logDisabled = true;
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
            return new LsmTree<>(this);
        }

    }

}
