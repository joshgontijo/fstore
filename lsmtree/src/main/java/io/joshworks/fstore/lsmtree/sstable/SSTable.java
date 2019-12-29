package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.lsmtree.Range;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;
import io.joshworks.fstore.lsmtree.sstable.entry.EntrySerializer;
import io.joshworks.fstore.lsmtree.sstable.filter.BloomFilter;
import io.joshworks.fstore.lsmtree.sstable.midpoints.Midpoint;
import io.joshworks.fstore.lsmtree.sstable.midpoints.Midpoints;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockIterator;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.File;
import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.Storage.EOF;
import static java.util.Objects.requireNonNull;

/**
 * A clustered index, key and values are kept together in the data area.
 * Midpoints and Bloom filter are kep in the footer area.
 * This segment uses {@link Segment} as the underlying storage. Therefore scan performed on a non readOnly is not permitted
 * block data that hasn't been persisted to disk, append will be buffered as it has to write to block first.
 * Any data that is not persisted will be lost when the store is closed or if the system crash.
 * This segment is intended to be used as persistent backend of an index, in which data is first stored in some sort
 * of transaction log, then added to this index so in case of failure this index can be rebuilt from the log
 *
 * @param <K> The Key type
 * @param <V> The value type
 */
public class SSTable<K extends Comparable<K>, V> implements Log<Entry<K, V>>, TreeFunctions<K, V> {

    private final Segment<Block> delegate;
    private final Serializer<Entry<K, V>> entrySerializer;
    private final Serializer<K> keySerializer;
    private final long maxEntrySize;
    private final Codec codec;
    private final int blockSize;
    private final BufferPool bufferPool;

    final BloomFilter bloomFilter;
    private final Midpoints<K> midpoints;

    private final long maxAge;

    private final Metrics metrics = new Metrics();

    private final Cache<String, Block> blockCache;

    private final Block block;

    private final Metadata metadata;

    SSTable(File file,
            StorageMode storageMode,
            long segmentDataSize,
            Serializer<K> keySerializer,
            Serializer<V> valueSerializer,
            BufferPool bufferPool,
            WriteMode writeMode,
            long maxAge,
            Codec codec,
            int maxEntrySize,
            Cache<String, Block> blockCache,
            long bloomNItems,
            double bloomFPProb,
            int blockSize,
            double checksumProb) {

        this.bufferPool = bufferPool;
        this.maxAge = maxAge;
        this.keySerializer = keySerializer;
        this.maxEntrySize = maxEntrySize;
        this.codec = codec;
        this.blockSize = blockSize;
        this.entrySerializer = EntrySerializer.of(maxAge, keySerializer, valueSerializer);
        this.block = Block.resizableVlenBlock().create(blockSize);

        this.blockCache = blockCache;
        this.delegate = new Segment<>(
                file,
                storageMode,
                segmentDataSize,
                new BlockSerializer(codec, Block.vlenBlock()),
                bufferPool,
                writeMode,
                checksumProb);

        if (delegate.readOnly()) {
            FooterReader reader = delegate.footerReader();
            this.midpoints = Midpoints.load(reader, codec, keySerializer);
            this.bloomFilter = BloomFilter.load(reader, codec, bufferPool);
            this.metadata = reader.read(Metadata.BLOCK_NAME, Metadata.serializer());
        } else {
            this.bloomFilter = BloomFilter.create(bloomNItems, bloomFPProb);
            this.midpoints = new Midpoints<>();
            this.metadata = new Metadata();
        }
    }

    private void addToBloomFilter(Entry<K, V> entry) {
        try (bufferPool) {
            ByteBuffer bb = bufferPool.allocate();
            keySerializer.writeTo(entry.key, bb);
            bb.flip();
            bloomFilter.add(bb);
        }
    }

    private void addToMidpoints(long position, Block block) {
        Entry<K, V> first = entrySerializer.fromBytes(block.first());
        Entry<K, V> last = entrySerializer.fromBytes(block.last());

        Midpoint<K> start = new Midpoint<>(first.key, position);
        Midpoint<K> end = new Midpoint<>(last.key, position);

        midpoints.add(start, end);
    }

    private void writeFooter(FooterWriter writer) {
        midpoints.writeTo(writer, codec, blockSize, bufferPool, keySerializer);
        bloomFilter.writeTo(writer, codec, blockSize, bufferPool);

        metadata.midpoints = midpoints.size();
        metadata.blocks = delegate.entries();
        writer.write(Metadata.BLOCK_NAME, metadata, Metadata.serializer());
    }

    @Override
    public long append(Entry<K, V> entry) {
        requireNonNull(entry, "Entry must be provided");
        requireNonNull(entry.key, "Entry Key must be provided");
        addToBloomFilter(entry);

        //block won't fit this segment, its state must be empty, otherwise data loss would occur
        if (delegate.remaining() < block.capacity()) {
            if (!block.isEmpty()) {
                throw new IllegalStateException("Segment has no available space for non empty block");
            }
            return EOF;
        }

        try (bufferPool) {
            ByteBuffer data = serialize(bufferPool, entry);

            if (data.remaining() > maxEntrySize) {
                throw new IllegalStateException("Entry of size " + data.remaining() + " cannot be greater than maxEntrySize: " + maxEntrySize);
            }

            //entry will not fit the segment, needs to roll without adding to block, it can be inserted in the next segment.
            //or it will exceed the limit that Segment can write at once, therefore throwing an exception
            if (!willFitSegment(data)) {
                return EOF;
            }

            if (exceedsMaxEntrySize(data) && !block.isEmpty()) {
                bufferPool.free();
                writeBlock(block);
                return append(entry);
            }

            //entry has to fit in the block
            if (!block.add(data)) {
                throw new IllegalStateException("Could not add entry to block");
            }
        }
        //block size full or expanded beyond blockSize
        if (block.position() > blockSize) {
            writeBlock(block);
        }

        metadata.entries++;
        return delegate.position();
    }

    private boolean willFitSegment(ByteBuffer data) {
        return delegate.remaining() > block.remaining() + data.remaining() + block.entryHeaderSize();
    }

    private boolean exceedsMaxEntrySize(ByteBuffer data) {
        return block.position() + data.remaining() + block.entryHeaderSize() > maxEntrySize;
    }

    private ByteBuffer serialize(BufferPool bufferPool, Entry<K, V> data) {
        ByteBuffer buffer = bufferPool.allocate();
        entrySerializer.writeTo(data, buffer);
        buffer.flip();
        return buffer;
    }

    private void writeBlock(Block block) {
        long pos = delegate.append(block);
        if (pos == EOF) {
            throw new RuntimeException("No space left in the sstable");
        }
        addToMidpoints(pos, block);
        block.clear();
    }

    @Override
    public Entry<K, V> get(long position) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> get(K key) {
        if (!readOnly()) {
            throw new IllegalStateException("Cannot read from a open segment");
        }
        try (bufferPool) {
            ByteBuffer bb = bufferPool.allocate();
            keySerializer.writeTo(key, bb);
            bb.flip();
            if (!bloomFilter.contains(bb)) {
                metrics.update("bloom.filtered");
                return null;
            }
        }
        Midpoint<K> midpoint = midpoints.getMidpointFor(key);
        if (midpoint == null) {
            return null;
        }

        Entry<K, V> found = readFromBlock(key, midpoint);
        if (found == null) {
            metrics.update("bloom.falsePositives");
        }
        return found;
    }

    K firstKey() {
        return midpoints.first().key;
    }

    K lastKey() {
        return midpoints.last().key;
    }

    Entry<K, V> first() {
        return getAt(midpoints.first(), true);
    }

    Entry<K, V> last() {
        return getAt(midpoints.last(), false);
    }

    //firstLast is a hacky way of getting either the first or last block element
    //true if first block element
    //false if last block element
    private Entry<K, V> getAt(Midpoint<K> midpoint, boolean firstLast) {
        Block block = readBlock(midpoint);
        ByteBuffer lastEntry = firstLast ? block.first() : block.last();
        return entrySerializer.fromBytes(lastEntry);
    }

    @Override
    public Entry<K, V> floor(K key) {
        if (midpoints.isEmpty()) {
            return null;
        }
        if (key.compareTo(midpoints.first().key) < 0) {
            return null;
        }

        int idx = midpoints.binarySearch(key);
        if (idx < 0 && Math.abs(idx) < 0) {
            return null;
        }
        idx = idx < 0 ? Math.abs(idx) - 2 : idx;
        if (idx < 0) {
            return null;
        }

        while (idx < midpoints.size()) {
            Midpoint<K> midpoint = midpoints.getMidpoint(idx++);
            Block block = readBlock(midpoint);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 2 : entryIdx;
            Entry<K, V> found = readNextNonExpired(block, entryIdx, Direction.BACKWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private Block readBlock(Midpoint<K> midpoint) {
        String cacheKey = cacheKey(midpoint.position);
        Block cached = blockCache.get(cacheKey);
        if (cached != null) {
            metrics.update("blockCache.hit");
            return cached;
        }
        metrics.update("blockCache.miss");
        metrics.update("block.read");
        Block block = delegate.get(midpoint.position);
        if (block == null) {
            throw new RuntimeException("Could not find block at position" + midpoint.position);
        }
        blockCache.add(cacheKey, block);
        return block;
    }

    @Override
    public Entry<K, V> lower(K key) {
        if (midpoints.isEmpty()) {
            return null;
        }
        if (key.compareTo(midpoints.first().key) <= 0) {
            return null;
        }

        int idx = midpoints.binarySearch(key);
        if (idx < 0 && Math.abs(idx) < 0) {
            return null;
        }
        idx = idx < 0 ? Math.abs(idx) - 2 : idx - 1;
        idx = Math.min(midpoints.size() - 1, idx);
        if (idx < 0) {
            return null;
        }
        while (idx < midpoints.size()) {
            Midpoint<K> midpoint = midpoints.getMidpoint(idx++);
            Block block = readBlock(midpoint);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 2 : entryIdx - 1;
            Entry<K, V> found = readNextNonExpired(block, entryIdx, Direction.BACKWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> ceiling(K key) {
        if (midpoints.isEmpty()) {
            return null;
        }
        if (key.compareTo(midpoints.last().key) > 0) {
            return null;
        }
        int idx = midpoints.binarySearch(key);
        idx = idx < 0 ? Math.abs(idx) - 2 : idx;
        idx = Math.max(0, idx);
        if (idx >= midpoints.size()) {
            return null;
        }
        while (idx < midpoints.size()) {
            Midpoint<K> midpoint = midpoints.getMidpoint(idx++);
            Block block = readBlock(midpoint);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 1 : entryIdx;
            Entry<K, V> found = readNextNonExpired(block, entryIdx, Direction.FORWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> higher(K key) {
        if (midpoints.isEmpty()) {
            return null;
        }
        if (key.compareTo(midpoints.last().key) >= 0) {
            return null;
        }

        int idx = midpoints.binarySearch(key);
        idx = idx < 0 ? Math.abs(idx) - 2 : idx;
        idx = Math.max(0, idx);
        if (idx >= midpoints.size()) {
            return null;
        }
        while (idx < midpoints.size()) {
            Midpoint<K> midpoint = midpoints.getMidpoint(idx++);
            Block block = readBlock(midpoint);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 1 : entryIdx + 1;
            Entry<K, V> found = readNextNonExpired(block, entryIdx, Direction.FORWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
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

    private Entry<K, V> readFromBlock(K key, Midpoint<K> midpoint) {
        Block block = readBlock(midpoint);
        return tryReadBlockEntry(key, block);
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
        return name() + position;
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(long position, Direction direction) {
        SegmentIterator<Block> iterator = delegate.iterator(position, direction);
        return new BlockIterator<>(entrySerializer, iterator, direction);
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(Direction direction) {
        SegmentIterator<Block> iterator = delegate.iterator(direction);
        return new BlockIterator<>(entrySerializer, iterator, direction);
    }

    public SegmentIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        if (!readOnly()) {
            throw new IllegalStateException("Cannot read from a open segment");
        }

        if (!intersect(range, midpoints)) {
            return SegmentIterator.empty();
        }

        long startPos = startPos(direction, range);
        SegmentIterator<Block> iterator = delegate.iterator(startPos, direction);
        BlockIterator<Entry<K, V>> blockIterator = new BlockIterator<>(entrySerializer, iterator, direction);
        return new RangeIterator<>(blockIterator, range, direction);
    }

    private long startPos(Direction direction, Range<K> range) {
        if (Direction.FORWARD.equals(direction)) {
            Midpoint<K> mStart = range.start() == null ? midpoints.first() : midpoints.getMidpointFor(range.start());
            return mStart == null ? midpoints.first().position : mStart.position;
        }
        if (range.end() == null) { //no start pos for backward scanner, start from the last block
            return midpoints.last().position;
        }
        //backwards scan needs the at the end of the block, so the start of the next block is used
        int idx = midpoints.getMidpointIdx(range.end());
        if (idx < 0) {
            return delegate.position(); //LOG_END
        }
        Midpoint<K> nextBlockPoint = midpoints.getMidpoint(idx + 1);
        return nextBlockPoint == null ? delegate.position() : nextBlockPoint.position;
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
        if (!block.isEmpty()) {
            writeBlock(block);
        }
        delegate.roll(level, trim, this::writeFooter);
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
        return metadata.entries;
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
        if (!block.isEmpty()) {
            writeBlock(block);
        }
        delegate.flush();
    }

    @Override
    public void close() {
        delegate.close();
    }

    public int midpoints() {
        return midpoints.size();
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public Metrics metrics() {
        Metrics metrics = Metrics.merge(this.metrics, delegate.metrics());
        metrics.set("bloom.size", bloomFilter.size());
        metrics.set("midpoint.entries", midpoints.size());
        return metrics;
    }

    static <T extends Comparable<T>> boolean intersect(Range<T> range, Midpoints<T> midpoints) {
        T x1 = range.start();
        T x2 = range.end();
        T y1 = midpoints.first().key;
        T y2 = midpoints.last().key;
        return between(x1, y1, y2) || between(x2, y1, y2)
                ||
                between(y1, x1, x2) || between(y2, x1, x2);
    }

    /**
     * Check whether x is between two values p1 and p2
     * if x is null, then true is returned based on the assumption that {@link Range#start()} or  {@link Range#end()} ()} can be null
     * if p1 is null, than the is assumed that there's no lower limit, therefore x must < p2
     * if p2 is null, than the is assumed that there's no upper limit, therefore x must >= p1
     */
    private static <T extends Comparable<T>> boolean between(T x, T p1, T p2) {
        if (x == null) {
            return true;
        }
        if (p1 == null) {
            return x.compareTo(p2) < 0;
        }
        if (p2 == null) {
            return x.compareTo(p1) >= 0;
        }
        return x.compareTo(p1) >= 0 && x.compareTo(p2) < 0;
    }

    private static class Metadata {
        private static final String BLOCK_NAME = "METADATA";

        private long entries;
        private long blocks;
        private long midpoints;

        public static Serializer<Metadata> serializer() {
            return new Serializer<>() {

                @Override
                public void writeTo(Metadata data, ByteBuffer dst) {
                    dst.putLong(data.entries);
                    dst.putLong(data.blocks);
                    dst.putLong(data.midpoints);
                }

                @Override
                public Metadata fromBytes(ByteBuffer buffer) {
                    Metadata metadata = new Metadata();
                    metadata.entries = buffer.getLong();
                    metadata.blocks = buffer.getLong();
                    metadata.midpoints = buffer.getLong();
                    return metadata;
                }
            };
        }
    }

    static class SSTableFactory<K extends Comparable<K>, V> implements SegmentFactory<Entry<K, V>> {
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private final Codec codec;
        private final long bloomNItems;
        private final double bloomFPProb;
        private final int blockSize;
        private final int maxEntrySize;
        private final long maxAge;
        private final Cache<String, Block> blockCache;

        SSTableFactory(Serializer<K> keySerializer,
                       Serializer<V> valueSerializer,
                       Codec codec,
                       long bloomNItems,
                       double bloomFPProb,
                       int blockSize,
                       int maxEntrySize,
                       long maxAge,
                       Cache<String, Block> blockCache) {

            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.codec = codec;
            this.bloomNItems = bloomNItems;
            this.bloomFPProb = bloomFPProb;
            this.blockSize = blockSize;
            this.maxEntrySize = maxEntrySize;
            this.maxAge = maxAge;
            this.blockCache = blockCache;
        }

        @Override
        public Log<Entry<K, V>> createOrOpen(File file, StorageMode storageMode, long dataLength, Serializer<Entry<K, V>> serializer, BufferPool bufferPool, WriteMode writeMode, double checksumProb) {
            return new SSTable<>(
                    file,
                    storageMode,
                    dataLength,
                    keySerializer,
                    valueSerializer,
                    bufferPool,
                    writeMode,
                    maxAge,
                    codec,
                    maxEntrySize,
                    blockCache,
                    bloomNItems,
                    bloomFPProb,
                    blockSize,
                    checksumProb);
        }

        @Override
        public Log<Entry<K, V>> mergeOut(File file, StorageMode storageMode, long dataLength, long entries, Serializer<Entry<K, V>> serializer, BufferPool bufferPool, WriteMode writeMode, double checksumProb) {
            return new SSTable<>(
                    file,
                    storageMode,
                    dataLength,
                    keySerializer,
                    valueSerializer,
                    bufferPool,
                    writeMode,
                    maxAge,
                    codec,
                    maxEntrySize,
                    blockCache,
                    entries,
                    bloomFPProb,
                    blockSize,
                    checksumProb);
        }
    }

}
