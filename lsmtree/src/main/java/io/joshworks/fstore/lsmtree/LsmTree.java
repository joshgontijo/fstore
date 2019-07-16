package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.index.Range;
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
import java.util.List;
import java.util.Objects;

public class LsmTree<K extends Comparable<K>, V> implements Closeable {

    private final SSTables<K, V> sstables;
    private final TransactionLog<K, V> log;
    private final int flushThreshold;
    private final boolean logDisabled;

    private MemTable<K, V> memTable;

    private LsmTree(Builder<K, V> builder) {
        this.sstables = createSSTable(builder);
        this.log = createTransactionLog(builder);
        this.memTable = new MemTable<>();
        this.flushThreshold = builder.flushThreshold;
        this.logDisabled = builder.logDisabled;
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
        if (memTable.size() >= flushThreshold) {
            flushMemTable(false);
            return true;
        }
        return false;
    }

    public V get(K key) {
        V found = memTable.get(key);
        if (found != null) {
            return found;
        }
        return sstables.get(key);
    }

    public Entry<K, V> firstFloor(K key) {
        Entry<K, V> memFloor = memTable.ceiling(key);
        if (memFloor != null) {
            return memFloor;
        }
        return sstables.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> floor = sstable.ceiling(key);
                if (floor != null) {
                    return floor;
                }
            }
            return null;
        });
    }

    public Entry<K, V> firstCeiling(K key) {
        Entry<K, V> memFloor = memTable.floor(key);
        if (memFloor != null) {
            return memFloor;
        }
        return sstables.applyToSegments(Direction.BACKWARD, segments -> {
            for (Log<Entry<K, V>> segment : segments) {
                if (segment.readOnly()) {
                    continue;
                }
                SSTable<K, V> sstable = (SSTable<K, V>) segment;
                Entry<K, V> floor = sstable.floor(key);
                if (floor != null) {
                    return floor;
                }
            }
            return null;
        });
    }


//    public Entry<K, V> floor(K key) {
//        Entry<K, V> floor = memTable.floor(key);
//        sstables.
//    }
//
//    public Entry<K, V> ceiling(K key) {
//        Entry<K, V> ceiling = memTable.ceiling(key);
//    }
//
//    public Entry<K, V> higher(K key) {
//        Entry<K, V> higher = memTable.higher(key);
//    }
//
//    public Entry<K, V> lower(K key) {
//        Entry<K, V> lower = memTable.lower(key);
//    }

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
        private int blockCacheSize = 100;
        private int blockCacheMaxAge = 120000;
        private boolean useKryo;
        private int segmentSize = Size.MB.ofInt(32);

        private Builder(File directory, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
            this.directory = directory;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        public Builder<K, V> flushThreshold(int flushThreshold) {
            this.flushThreshold = flushThreshold;
            return this;
        }

        public Builder<K, V> bloomFalsePositiveProbability(double bloomFPProb) {
            this.bloomFPProb = bloomFPProb;
            return this;
        }

        public Builder<K, V> bloomNumItems(long bloomNItems) {
            this.bloomNItems = bloomNItems;
            return this;
        }

        public Builder<K, V> codec(Codec codec) {
            this.codec = codec;
            return this;
        }

        public Builder<K, V> blockSize(int blockSize) {
            this.blockSize = blockSize;
            return this;
        }

        public Builder<K, V> blockCacheSize(int blockCacheSize) {
            this.blockCacheSize = blockCacheSize;
            return this;
        }

        public Builder<K, V> segmentSize(int size) {
            this.segmentSize = size;
            return this;
        }

        public Builder<K, V> blockCacheMaxAge(int maxAgeSeconds) {
            this.blockCacheMaxAge = maxAgeSeconds * 1000;
            return this;
        }

        public Builder<K, V> disableTransactionLog() {
            this.logDisabled = true;
            return this;
        }

        public Builder<K, V> sstableStorageMode(StorageMode mode) {
            Objects.requireNonNull(mode);
            this.sstableStorageMode = mode;
            return this;
        }

        public Builder<K, V> sstableBlockFactory(BlockFactory blockFactory) {
            Objects.requireNonNull(blockFactory);
            this.sstableBlockFactory = blockFactory;
            return this;
        }

        public Builder<K, V> ssTableFlushMode(FlushMode mode) {
            Objects.requireNonNull(mode);
            this.ssTableFlushMode = mode;
            return this;
        }

        public Builder<K, V> transacationLogStorageMode(StorageMode mode) {
            Objects.requireNonNull(mode);
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
