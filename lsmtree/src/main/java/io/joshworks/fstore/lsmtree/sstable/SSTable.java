package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;

import static java.util.Objects.requireNonNull;

/**
 * A clustered index, key and values are kept together in the data area.
 * Midpoints and Bloom filter are kep in the footer area.
 * This segment uses {@link BlockSegment} as the underlying storage. Therefore scan performed on a non readOnly is not permitted
 * block data that hasn't been persisted to disk, append will be buffered as it has to write to block first.
 * Any data that is not persisted will be lost when the store is closed or if the system crash.
 * This segment is intended to be used as persistent backend of an index, in which data is first stored in some sort
 * of transaction log, then added to this index so in case of failure this index can be rebuilt from the log
 *
 * @param <K> The Key type
 * @param <V> The value type
 */
public class SSTable<K extends Comparable<K>, V> implements Log<Entry<K, V>>, TreeFunctions<K, V> {

    private final BlockSegment<Entry<K, V>> delegate;
    private final Serializer<Entry<K, V>> entrySerializer;
    private final Serializer<K> keySerializer;
    private final Codec codec;
    private final BufferPool bufferPool;

    private final Index<K> index;

    private final long maxAge;

    private final Metrics metrics = new Metrics();

    private final Cache<String, Block> blockCache;

    private final MemTable<K, V> memTable = new MemTable<>();


    public SSTable(File file,
                   StorageMode storageMode,
                   long segmentDataSize,
                   Serializer<K> keySerializer,
                   Serializer<V> valueSerializer,
                   BufferPool bufferPool,
                   WriteMode writeMode,
                   BlockFactory blockFactory,
                   long maxAge,
                   Codec codec,
                   Cache<String, Block> blockCache,
                   double bloomFPProb,
                   int blockSize,
                   double checksumProb,
                   int readPageSize) {

        this.bufferPool = bufferPool;
        this.maxAge = maxAge;
        this.keySerializer = keySerializer;
        this.codec = codec;
        this.entrySerializer = EntrySerializer.of(maxAge, keySerializer, valueSerializer);

        this.blockCache = blockCache;
        this.delegate = new BlockSegment<>(
                file,
                storageMode,
                segmentDataSize,
                bufferPool,
                writeMode,
                entrySerializer,
                blockFactory,
                codec,
                blockSize,
                checksumProb,
                readPageSize,
                (a, b) -> {
                },
                (a, b) -> {
                },
                this::writeFooter);



        this.index = new Index<>(file, delegate.readOnly(), bufferPool, keySerializer, maxAge, bloomFPProb);
    }

    private void writeFooter(FooterWriter writer) {
        index.writeTo(writer);
    }

    @Override
    public long append(Entry<K, V> data) {
        requireNonNull(data, "Entry must be provided");
        requireNonNull(data.key, "Entry Key must be provided");
        long position = delegate.append(data);

        long pos = data.deletion() ? -(position) : position;
        index.add(data.key, pos, data.timestamp);
        return position;
    }

    @Override
    public Entry<K, V> get(long position) {
        if (!readOnly()) {
            throw new IllegalStateException("Cannot read from a open segment");
        }
        return delegate.get(position);
    }

    public Entry<K, V> get(K key) {
        if (!readOnly()) {
            throw new IllegalStateException("Cannot read from a open segment");
        }

        Entry<K, V> found = readFromBlock(key);
        if (found == null) {
            metrics.update("bloom.falsePositives");
        }
        return found;
    }

    public K firstKey() {
        return index.firstKey();
    }

    public K lastKey() {
        return index.lastKey();
    }

    public Entry<K, V> first() {
        IndexEntry<K> first = index.first();
        return readFromBlock(first.key);
    }

    public Entry<K, V> last() {
        IndexEntry<K> last = index.last();
        return readFromBlock(last.key);
    }

    private Block readBlock(long position) {
        metrics.update("block.read");
        return delegate.getBlock(position);
    }

    @Override
    public Entry<K, V> floor(K key) {
        IndexEntry<K> ientry = index.floor(key);
        if(ientry == null) {
            return null;
        }
        return readEntry(ientry);
    }

    @Override
    public Entry<K, V> lower(K key) {
        IndexEntry<K> ientry = index.lower(key);
        if(ientry == null) {
            return null;
        }
        return readEntry(ientry);
    }

    @Override
    public Entry<K, V> ceiling(K key) {
        IndexEntry<K> ientry = index.ceiling(key);
        if(ientry == null) {
            return null;
        }
        return readEntry(ientry);
    }

    @Override
    public Entry<K, V> higher(K key) {
        IndexEntry<K> ientry = index.higher(key);
        if(ientry == null) {
            return null;
        }
        return readEntry(ientry);
    }

    private int binarySearch(Block block, K key) {
        int low = 0;
        int high = block.entryCount() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int cmp = compareTo(block.get(mid), key);
            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found
    }

    private int compareTo(ByteBuffer entry, K key) {
        int prevPos = entry.position();
        K entryKey = keySerializer.fromBytes(entry);
        entry.position(prevPos);
        return entryKey.compareTo(key);
    }

    private Entry<K, V> readNextNonExpired(Block block, int idx, Direction direction) {
        while (idx >= 0 && idx < block.entryCount()) {
            Entry<K, V> found = readNonExpired(block, idx);
            if (found != null) {
                return found;
            }
            idx = Direction.FORWARD.equals(direction) ? idx + 1 : idx - 1;
        }
        return null;
    }

    private Entry<K, V> readNonExpired(Block block, int i) {
        ByteBuffer data = block.get(i);
        Entry<K, V> entry = entrySerializer.fromBytes(data);
        return entry.readable(maxAge) ? entry : null;
    }

    private Entry<K, V> readFromBlock(K key) {
        IndexEntry<K> indexEntry = index.get(key);
        if (indexEntry == null) {
            return null;
        }
        return readEntry(indexEntry);
    }

    private Entry<K, V> readEntry(IndexEntry<K> indexEntry) {
        String cacheKey = cacheKey(indexEntry.value);
        Block cached = blockCache.get(cacheKey);
        if (cached == null) {
            metrics.update("blockCache.miss");

            Block block = readBlock(indexEntry.value);
            if (block == null) {
                return null;
            }
            Entry<K, V> entry = tryReadBlockEntry(indexEntry.key, block);
            if (entry != null) {
                blockCache.add(cacheKey, block);
            }
            return entry;
        } else {
            metrics.update("blockCache.hit");
        }
        return tryReadBlockEntry(indexEntry.key, cached);
    }

    private Entry<K, V> tryReadBlockEntry(K key, Block block) {
        int idx = binarySearch(block, key);
        if (idx < 0) {
            return null;
        }
        ByteBuffer entryData = block.get(idx);
        Entry<K, V> entry = entrySerializer.fromBytes(entryData);
        return entry.readable(maxAge) ? entry : null;
    }


    private String cacheKey(long position) {
        return name() + "_" + position;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(long position, Direction direction) {
        return new SSTableIterator<>(maxAge, delegate.iterator(position, direction));
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(Direction direction) {
        return new SSTableIterator<>(maxAge, delegate.iterator(direction));
    }

    public SegmentIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        if (!readOnly()) {
            throw new IllegalStateException("Cannot read from a open segment");
        }
        if (!range.intersects(index.firstKey(), index.lastKey())) {
            return SegmentIterator.empty();
        }

        long startPos = index.startPos(direction, range);
        SegmentIterator<Entry<K, V>> iterator = delegate.iterator(startPos, direction);
        return new RangeIterator<>(maxAge, iterator, range, direction);
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public long physicalSize() {
        return delegate.physicalSize();
    }

    @Override
    public long logicalSize() {
        return delegate.logicalSize();
    }

    @Override
    public long dataSize() {
        return delegate.dataSize();
    }

    @Override
    public long actualDataSize() {
        return delegate.actualDataSize();
    }

    @Override
    public long uncompressedDataSize() {
        return delegate.actualDataSize();
    }

    @Override
    public long headerSize() {
        return delegate.headerSize();
    }

    @Override
    public long footerSize() {
        return delegate.footerSize();
    }

    @Override
    public long remaining() {
        return delegate.remaining();
    }

    @Override
    public void delete() {
        delegate.delete();
    }

    @Override
    public void roll(int level, boolean trim) {
        delegate.roll(level, trim);
    }

    @Override
    public boolean readOnly() {
        return delegate.readOnly();
    }

    @Override
    public boolean closed() {
        return delegate.closed();
    }

    @Override
    public long entries() {
        return delegate.entries();
    }

    @Override
    public int level() {
        return delegate.level();
    }

    @Override
    public long created() {
        return delegate.created();
    }

    @Override
    public long uncompressedSize() {
        return delegate.uncompressedSize();
    }

    @Override
    public Type type() {
        return delegate.type();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public Metrics metrics() {
        Metrics metrics = Metrics.merge(this.metrics, delegate.metrics());
        return Metrics.merge(metrics, index.metrics());
    }

    static class SSTableFactory<K extends Comparable<K>, V> implements SegmentFactory<Entry<K, V>> {
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private final BlockFactory blockFactory;
        private final Codec codec;
        private final double bloomFPProb;
        private final int blockSize;
        private final long maxAge;
        private final Cache<String, Block> blockCache;

        SSTableFactory(Serializer<K> keySerializer,
                       Serializer<V> valueSerializer,
                       BlockFactory blockFactory,
                       Codec codec,
                       double bloomFPProb,
                       int blockSize,
                       long maxAge,
                       Cache<String, Block> blockCache) {

            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.blockFactory = blockFactory;
            this.codec = codec;
            this.bloomFPProb = bloomFPProb;
            this.blockSize = blockSize;
            this.maxAge = maxAge;
            this.blockCache = blockCache;
        }

        @Override
        public Log<Entry<K, V>> createOrOpen(File file, StorageMode storageMode, long dataLength, Serializer<Entry<K, V>> serializer, BufferPool bufferPool, WriteMode writeMode, double checksumProb, int readPageSize) {
            return new SSTable<>(
                    file,
                    storageMode,
                    dataLength,
                    keySerializer,
                    valueSerializer,
                    bufferPool,
                    writeMode,
                    blockFactory,
                    maxAge,
                    codec,
                    blockCache,
                    bloomFPProb,
                    blockSize,
                    checksumProb,
                    readPageSize);
        }
    }
}
