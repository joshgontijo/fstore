package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.cache.Cache;
import io.joshworks.fstore.index.filter.BloomFilter;
import io.joshworks.fstore.index.midpoints.Midpoint;
import io.joshworks.fstore.index.midpoints.Midpoints;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.log.segment.header.Type;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;

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
    private final BufferPool bufferPool;

    private final Codec footerCodec;
    private final BloomFilter bloomFilter;
    private final Midpoints<K> midpoints;

    private final long maxAge;

    private final Cache<String, Block> blockCache;

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
                   Codec footerCodec,
                   Cache<String, Block> blockCache,
                   long bloomNItems,
                   double bloomFPProb,
                   int blockSize,
                   double checksumProb,
                   int readPageSize) {

        this.bufferPool = bufferPool;
        this.maxAge = maxAge;
        this.footerCodec = footerCodec;
        this.keySerializer = keySerializer;
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
                this::onBlockWrite,
                this::onBlockLoaded,
                this::writeFooter);

        if (delegate.readOnly()) {
            FooterReader reader = delegate.footerReader();
            this.midpoints = Midpoints.load(reader, footerCodec, bufferPool, keySerializer);
            this.bloomFilter = BloomFilter.load(reader, footerCodec, bufferPool);
        } else {
            this.bloomFilter = BloomFilter.create(bloomNItems, bloomFPProb);
            this.midpoints = new Midpoints<>();
        }
    }

    private void onBlockLoaded(long position, Block block) {
        addToMidpoints(position, block);
        List<Entry<K, V>> entries = block.deserialize(entrySerializer);
        for (Entry<K, V> entry : entries) {
            addToBloomFilter(entry);
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

    private void onBlockWrite(long position, Block block) {
        addToMidpoints(position, block);
    }

    private void addToMidpoints(long position, Block block) {
        Entry<K, V> first = entrySerializer.fromBytes(block.first());
        Entry<K, V> last = entrySerializer.fromBytes(block.last());

        Midpoint<K> start = new Midpoint<>(first.key, position);
        Midpoint<K> end = new Midpoint<>(last.key, position);

        midpoints.add(start, end);
    }

    private void writeFooter(FooterWriter writer) {
        midpoints.writeTo(writer, footerCodec, bufferPool, keySerializer);
        bloomFilter.writeTo(writer, footerCodec, bufferPool);
    }

    @Override
    public long append(Entry<K, V> data) {
        requireNonNull(data, "Entry must be provided");
        requireNonNull(data.key, "Entry Key must be provided");
        addToBloomFilter(data);
        return delegate.append(data);
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
        try (bufferPool) {
            ByteBuffer bb = bufferPool.allocate();
            keySerializer.writeTo(key, bb);
            bb.flip();
            if (!bloomFilter.contains(bb)) {
                return null;
            }
        }
        Midpoint<K> midpoint = midpoints.getMidpointFor(key);
        if (midpoint == null) {
            return null;
        }

        return readFromBlock(key, midpoint);
    }

    public K firstKey() {
        return midpoints.first().key;
    }

    public K lastKey() {
        return midpoints.last().key;
    }

    public Entry<K, V> first() {
        return getAt(midpoints.first(), true);
    }

    public Entry<K, V> last() {
        return getAt(midpoints.last(), false);
    }

    //firstLast is a hacky way of getting either the first or last block element
    //true if first block element
    //false if last block element
    private Entry<K, V> getAt(Midpoint<K> midpoint, boolean firstLast) {
        Block block = delegate.getBlock(midpoint.position);
        ByteBuffer lastEntry = firstLast ? block.first() : block.last();
        return entrySerializer.fromBytes(lastEntry);
    }

    @Override
    public Entry<K, V> floor(K key) {
        if (midpoints.isEmpty()) {
            return null;
        }
        int idx = midpoints.binarySearch(key);
        if (idx < 0 && Math.abs(idx) <= 0) {
            return null;
        }
        idx = idx < 0 ? Math.abs(idx) - 2 : idx;

        while (idx < midpoints.size()) {
            Midpoint<K> midpoint = midpoints.getMidpoint(idx++);
            Block block = delegate.getBlock(midpoint.position);
            int bidx = binarySearch(block, key);
            if (bidx >= 0) {
                Entry<K, V> found = readNextNonExpired(block, bidx, Direction.BACKWARD);
                if(found != null) {
                    return found;
                }
            }
            if (bidx < 0 && Math.abs(bidx) <= 0) {
                return null;
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> ceiling(K key) {
        if (midpoints.isEmpty()) {
            return null;
        }
        int midpointIdx = midpoints.getMidpointIdx(key);
        midpointIdx = midpointIdx < 0 ? 0 : midpointIdx;

        Midpoint<K> midpoint = midpoints.getMidpoint(midpointIdx);
        Block block = delegate.getBlock(midpoint.position);
        Entry<K, V> ceiling = ceilingEntry(key, block);
        if (ceiling != null) {
            return ceiling;
        }

        //not in block get next one
        if (midpointIdx == midpoints.size() - 1) { //last midpoint, nothing in this segment
            return null;
        }
        //has next midpoint, fetch first entry of next block
        Midpoint<K> next = midpoints.getMidpoint(midpointIdx + 1);
        Block nextBlock = delegate.getBlock(next.position);
        return entrySerializer.fromBytes(nextBlock.first());
    }

    @Override
    public Entry<K, V> higher(K key) {
        //less than first entry, definitely first entry
        if (key.compareTo(midpoints.first().key) < 0) {
            return first();
        }
        //in range
        int midpointIdx = midpoints.getMidpointIdx(key);
        Midpoint<K> midpoint = midpoints.getMidpoint(midpointIdx);
        Block block = delegate.getBlock(midpoint.position);
        Entry<K, V> ceiling = higherEntry(key, block);
        if (ceiling != null) {
            return ceiling;
        }

        //not in block get next one
        if (midpointIdx == midpoints.size() - 1) { //last midpoint, nothing in this segment
            return null;
        }
        //has next midpoint, fetch first entry of next block
        Midpoint<K> next = midpoints.getMidpoint(midpointIdx + 1);
        Block nextBlock = delegate.getBlock(next.position);
        return entrySerializer.fromBytes(nextBlock.first());
    }

    @Override
    public Entry<K, V> lower(K key) {
        int idx = midpoints.binarySearch(key);
        //exact match with the first element of the block, get last element from the previous block
        //only do this for block before the last one, as the last two midpoints are the same block
        if (idx > 0 && idx < midpoints.size() - 1) {
            Midpoint<K> midpoint = midpoints.getMidpoint(idx - 1);
            Block block = delegate.getBlock(midpoint.position);
            return entrySerializer.fromBytes(block.last());
        }
        idx = idx < 0 ? Math.abs(idx) - 2 : idx;
        Midpoint<K> midpoint = midpoints.getMidpoint(idx);
        Block block = delegate.getBlock(midpoint.position);

        for (int i = 0; i < block.entryCount(); i++) {
            ByteBuffer entryData = block.get(i);
            int compare = compareKey(entryData, key);
            if (compare <= 0) { //key is less than or equals
                if (i == 0) {//less than first entry, definitely not in this segment
                    return null;
                }
                //key is less than current item, and there's previous one, return previous one
                ByteBuffer floorEntry = block.get(i - 1);
                floorEntry.clear();
                return entrySerializer.fromBytes(floorEntry);
            }
            //key greater or equals, continue...
        }
        //greater than last block entry, definitely last block entry
        return entrySerializer.fromBytes(block.last());
    }

    Entry<K, V> applyToBlock(Block block, K key, Expression exp) {
        for (int i = 0; i < block.entryCount(); i++) {
            ByteBuffer entryData = block.get(i);
            int compare = compareKey(entryData, key);

            //key is less than current entry block
            if (compare < 0) {
                //less than first entry in the block
                if (i == 0 && (Expression.FLOOR.equals(exp) || Expression.LOWER.equals(exp))) {
                    return null;
                }
                if (Expression.CEILING.equals(exp) || Expression.HIGHER.equals(exp)) {
                    return readNextNonExpired(block, i, Direction.FORWARD);
                }
            }

            //key equals to current item
            if (compare == 0) {
                if (Expression.EQUALS.equals(exp)) {
                    //block will not have duplicated keys, no need to read any other entry
                    return readNonExpired(block, i);
                }

                if (Expression.FLOOR.equals(exp)) {
                    return readNextNonExpired(block, i, Direction.BACKWARD);
                }
                if (Expression.LOWER.equals(exp)) {
                    return readNextNonExpired(block, i - 1, Direction.BACKWARD);
                }
                if (Expression.CEILING.equals(exp)) {
                    return readNextNonExpired(block, i, Direction.FORWARD);
                }
                if (Expression.HIGHER.equals(exp)) {
                    return readNextNonExpired(block, i + 1, Direction.FORWARD);
                }
            }

            //key is greater than current entry
            if (compare > 0) {
                if (Expression.EQUALS.equals(exp)) {
                    //pass by the entry and wasn't present, from now only greater entries will be returned
                    return null;
                }
                //less than first entry in the block
                if (i == block.entryCount() && (Expression.CEILING.equals(exp) || Expression.HIGHER.equals(exp))) {
                    return null;
                }
                if (Expression.FLOOR.equals(exp) || Expression.LOWER.equals(exp)) {
                    return readNextNonExpired(block, i, Direction.BACKWARD);
                }
            }
        }
        return null;
    }

    public int binarySearch(Block block, K key) {
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
        return entry.expired(maxAge) ? null : entry;
    }


    private Entry<K, V> ceilingEntry(K key, Block block) {
        for (int i = block.entryCount() - 1; i >= 0; i--) {
            ByteBuffer entryData = block.get(i);
            int compare = compareKey(entryData, key);
            if (compare > 0) { //key is greater tha current
                //not in this block, is in the next get first entry of next one
                //returning null will cause caller to get next block
                if (i == block.entryCount() - 1) {
                    return null;
                }
                ByteBuffer entry = block.get(i + 1);
                entry.clear();
                Entry<K, V> found = entrySerializer.fromBytes(entry);
                if (!found.expired(maxAge)) {
                    return found;
                }
            }
            if (compare == 0) { //key equals to current item, return it
                ByteBuffer entry = block.get(i);
                entry.clear();
                Entry<K, V> found = entrySerializer.fromBytes(entry);
                if (!found.expired(maxAge)) {
                    return found;
                }
            }
        }
        //less than first block entry, definitely first block entry
        return entrySerializer.fromBytes(block.first());
    }

    private Entry<K, V> higherEntry(K key, Block block) {
        for (int i = block.entryCount() - 1; i >= 0; i--) {
            ByteBuffer entryData = block.get(i);
            int compare = compareKey(entryData, key);
            if (compare >= 0) { //key is greater tha current
                //not in this block, is in the next get first entry of next one
                //returning null will cause caller to get next block
                if (i == block.entryCount() - 1) {
                    return null;
                }
                ByteBuffer entry = block.get(i + 1);
                entry.clear();
                Entry<K, V> found = entrySerializer.fromBytes(entry);
                if (!found.expired(maxAge)) {
                    return found;
                }
            }
        }
        //less than first block entry, definitely first block entry
        return entrySerializer.fromBytes(block.first());
    }

    private Entry<K, V> readFromBlock(K key, Midpoint<K> midpoint) {
        String cacheKey = cacheKey(midpoint.position);
        Block cached = blockCache.get(cacheKey);
        if (cached == null) {
            Block block = delegate.getBlock(midpoint.position);
            if (block == null) {
                return null;
            }
            blockCache.add(cacheKey, block);
            return findExact(key, block);
        }
        return findExact(key, cached);
    }

    private Entry<K, V> findExact(K key, Block block) {
//        ByteBuffer keyBytes = keySerializer.toBytes(key);
        for (ByteBuffer entryData : block) {
            int compare = compareKey(entryData, key);
            if (compare == 0) {
                entryData.clear();
                Entry<K, V> entry = entrySerializer.fromBytes(entryData);
                return entry;
            }
            if (compare < 0) { //not found, short circuit (block entries are ordered)
                return null;
            }
        }
        return null;
    }

    //Key must be the first serialized field in order for this method to work
    private int compareKey(ByteBuffer entry, K key) {
        int prevPos = entry.position();
        K entryKey = keySerializer.fromBytes(entry);
        entry.position(prevPos);
        return key.compareTo(entryKey);
    }

    public SegmentIterator<Entry<K, V>> iterator(Direction direction, Range<K> range) {
        if (!readOnly()) {
            throw new IllegalStateException("Cannot read from a open segment");
        }
        if ((range.start() != null && !midpoints.inRange(range.start())) && (range.end() != null && !midpoints.inRange(range.end()))) {
            return SegmentIterator.empty();
        }

        long startPos = startPos(direction, range);
        SegmentIterator<Entry<K, V>> iterator = iterator(startPos, direction);
        return new RangeIterator<>(range, direction, iterator);

    }

    private long startPos(Direction direction, Range<K> range) {
        if (Direction.FORWARD.equals(direction)) {
            Midpoint<K> mStart = range.start() == null ? midpoints.first() : midpoints.getMidpointFor(range.start());
            return mStart == null ? midpoints.first().position : mStart.position;
        }
        Midpoint<K> mEnd = range.end() == null ? midpoints.last() : midpoints.getMidpointFor(range.end());
        return mEnd == null ? midpoints.last().position : mEnd.position;

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
        return delegate.iterator(position, direction);
    }

    @Override
    public SegmentIterator<Entry<K, V>> iterator(Direction direction) {
        return delegate.iterator(direction);
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

    static class SSTableFactory<K extends Comparable<K>, V> implements SegmentFactory<Entry<K, V>> {
        private final Serializer<K> keySerializer;
        private final Serializer<V> valueSerializer;
        private final BlockFactory blockFactory;
        private final Codec codec;
        private final Codec footerCodec;
        private final long bloomNItems;
        private final double bloomFPProb;
        private final int blockSize;
        private final long maxAge;
        private Cache<String, Block> blockCache;

        SSTableFactory(Serializer<K> keySerializer,
                       Serializer<V> valueSerializer,
                       BlockFactory blockFactory,
                       Codec codec,
                       Codec footerCodec,
                       long bloomNItems,
                       double bloomFPProb,
                       int blockSize,
                       long maxAge,
                       Cache<String, Block> blockCache) {

            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
            this.blockFactory = blockFactory;
            this.codec = codec;
            this.footerCodec = footerCodec;
            this.bloomNItems = bloomNItems;
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
                    footerCodec,
                    blockCache,
                    bloomNItems,
                    bloomFPProb,
                    blockSize,
                    checksumProb,
                    readPageSize);
        }
    }

}
