package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.compaction.combiner.UniqueMergeCombiner;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.lsmtree.log.LogRecord;
import io.joshworks.fstore.lsmtree.log.NoOpTransactionLog;
import io.joshworks.fstore.lsmtree.log.PersistentTransactionLog;
import io.joshworks.fstore.lsmtree.log.TransactionLog;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.lsmtree.sstable.Expression;
import io.joshworks.fstore.lsmtree.sstable.MemTable;
import io.joshworks.fstore.lsmtree.sstable.SSTable;
import io.joshworks.fstore.lsmtree.sstable.SSTableCompactor;
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
    private final boolean flushOnClose;

    private MemTable<K, V> memTable;
    private final long maxAge;

    private LsmTree(Builder<K, V> builder) {
        this.maxAge = builder.maxAgeSeconds;
        this.sstables = createSSTable(builder);
        this.log = createTransactionLog(builder);
        this.memTable = new MemTable<>();
        this.flushThreshold = builder.flushThreshold;
        this.logDisabled = builder.logDisabled;
        this.flushOnClose = builder.flushOnClose;
        this.log.restore(this::restore);
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
                builder.sstableCompactor,
                builder.maxAgeSeconds,
                builder.codec,
                builder.footerCodec,
                builder.bloomNItems,
                builder.bloomFPProb,
                builder.blockSize,
                builder.blockCache);
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
        requireNonNull(key, "Key must be provided");
        requireNonNull(value, "Value must be provided");
        LogRecord<K, V> record = LogRecord.add(key, value);
        log.append(record);

        Entry<K, V> entry = Entry.add(key, value);
        memTable.add(entry);
        if (memTable.size() >= flushThreshold) {
            flushMemTable(false);
            return true;
        }
        return false;
    }

    public V get(K key) {
        Entry<K, V> entry = getEntry(key);
        return entry == null ? null : entry.value;
    }

    public Entry<K, V> getEntry(K key) {
        requireNonNull(key, "Key must be provided");

        Entry<K, V> fromMem = memTable.get(key);
        if (fromMem != null) {
            return fromMem;
        }
        return sstables.get(key);
    }

    public void remove(K key) {
        requireNonNull(key, "Key must be provided");
        log.append(LogRecord.delete(key));
        memTable.add(Entry.delete(key));
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
        requireNonNull(key, "Key must be provided");
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
        return entry != null && !entry.deletion() && matcher.test(entry);
    }

    public long size() {
        return sstables.size();
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction) {
        List<CloseableIterator<Entry<K, V>>> segmentsIterators = sstables.segmentsIterator(direction);
        return new LsmTreeIterator<>(segmentsIterators, memTable.iterator(direction));
    }

    public CloseableIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        List<CloseableIterator<Entry<K, V>>> segmentsIterators = sstables.segmentsIterator(direction, range);
        return new LsmTreeIterator<>(segmentsIterators, memTable.iterator(direction, range));
    }

    @Override
    public void close() {
        if (logDisabled && !memTable.isEmpty()) {
            flushMemTable(flushOnClose);
        }
        sstables.close();
        log.close();
    }

    private synchronized void flushMemTable(boolean force) {
        if (!force && memTable.size() < flushThreshold) {
            return;
        }
        MemTable<K, V> tmp = memTable;
        long inserted = tmp.writeTo(sstables, maxAge);
        if (inserted > 0) {
            log.markFlushed();
        }
        memTable = new MemTable<>();
    }

    private void restore(LogRecord<K, V> record) {
        memTable.add(Entry.of(record.timestamp, record.key, record.value));
    }

    public void compact() {
        flushMemTable(true);
        sstables.compact();
    }

    public static class Builder<K extends Comparable<K>, V> {

        private static final int DEFAULT_THRESHOLD = 1000000;
        private static final int NO_MAX_AGE = -1;

        private final File directory;
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private Codec footerCodec = Codec.noCompression();
        private UniqueMergeCombiner<Entry<K, V>> sstableCompactor;
        private long bloomNItems = DEFAULT_THRESHOLD;
        private double bloomFPProb = 0.05;
        private int blockSize = Memory.PAGE_SIZE;
        private int flushThreshold = DEFAULT_THRESHOLD;
        private boolean logDisabled;
        private Codec codec = new SnappyCodec();
        private String name = "lsm-tree";
        private StorageMode sstableStorageMode = StorageMode.MMAP;
        private FlushMode ssTableFlushMode = FlushMode.ON_ROLL;
        private BlockFactory sstableBlockFactory = Block.vlenBlock();
        private StorageMode tlogStorageMode = StorageMode.RAF;
        private long segmentSize = Size.MB.of(32);

        private Cache<String, Block> blockCache = Cache.lruCache(1000, -1);
        private boolean flushOnClose = true;
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
            this.bloomNItems = bloomNItems;
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

        public Builder<K, V> footerCodec(Codec footerCodec) {
            requireNonNull(footerCodec, "Codec cannot be null");
            this.footerCodec = footerCodec;
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

        public Builder<K, V> disableTransactionLog() {
            this.logDisabled = true;
            return this;
        }

        public Builder<K, V> flushOnClose(boolean flushOnClose) {
            this.flushOnClose = flushOnClose;
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
