package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.codec.snappy.LZ4Codec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.lsmtree.sstable.filter.BloomFilter;
import io.joshworks.fstore.lsmtree.sstable.midpoints.Midpoint;
import io.joshworks.fstore.lsmtree.sstable.midpoints.Midpoints;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class Index<K extends Comparable<K>> implements TreeFunctions<K, Long>{

    private final FooterReader reader;
    private final BufferPool bufferPool;
    private final Serializer<IndexEntry<K>> entrySerializer;
    private final Serializer<K> keySerializer;

    private BloomFilter bloomFilter;
    private Midpoints<K> midpoints = new Midpoints<>();
    private List<IndexEntry<K>> items = new ArrayList<>();
    private final Codec codec = new LZ4Codec();
    private boolean readOnly;
    private final long maxAge;
    private final double bloomFilterFP;

    private final Metrics metrics = new Metrics();
    private final BlockFactory blockFactory;
    private final BlockSerializer blockSerializer;

    private final Cache<Long, Block> indexBlockCache = Cache.softCache();

    public Index(FooterReader reader, boolean readOnly, BufferPool bufferPool, Serializer<K> keySerializer, long maxAge, double bloomFilterFP) {
        this.reader = reader;
        this.bufferPool = bufferPool;
        this.maxAge = maxAge;
        this.bloomFilterFP = bloomFilterFP;
        this.readOnly = readOnly;

        this.blockFactory = Block.vlenBlock();
        this.blockSerializer = new BlockSerializer(codec, blockFactory);

        this.entrySerializer = new IndexEntrySerializer<>(keySerializer);
        this.keySerializer = keySerializer;
        //try read first block to detect whether the index exists
        if (readOnly) {
            this.midpoints = Midpoints.load(reader, codec, keySerializer);
            this.bloomFilter = BloomFilter.load(reader, codec, bufferPool);
        }
    }

    void add(K key, long dataBlockPos, long timestamp) {
        items.add(new IndexEntry<>(timestamp, key, dataBlockPos));
    }

    private void addToBloomFilter(K key) {
        try (bufferPool) {
            ByteBuffer bb = bufferPool.allocate();
            keySerializer.writeTo(key, bb);
            bb.flip();
            bloomFilter.add(bb);
        }
    }

    @Override
    public IndexEntry<K> get(K key) {
        if (!readOnly) {
            throw new IllegalStateException("Cannot read from a open segment");
        }
        try (bufferPool) {
            ByteBuffer bb = bufferPool.allocate();
            keySerializer.writeTo(key, bb);
            bb.flip();
            if (!bloomFilter.contains(bb)) {
//                metrics.update("bloom.filtered");
                return null;
            }
        }
        Long blockPos = blockPos(key);
        if (blockPos == null) {
            return null;
        }

        IndexEntry<K> found = readIndexBlock(key, blockPos);
        if (found == null) {
//            metrics.update("bloom.falsePositives");
        }
        return found;
    }

    private IndexEntry<K> readIndexBlock(K key, long position) {
        Block block = readBlock(position);
        int bPos = binarySearch(block, key);
        if (bPos < 0) {
            return null;
        }
        ByteBuffer entryData = block.get(bPos);
        IndexEntry<K> entry = entrySerializer.fromBytes(entryData);
        return entry.readable(maxAge) ? entry : null;
    }

    private Block readBlock(long position) {
        Block block = indexBlockCache.get(position);
        if(block == null) {
            block = reader.read(position, blockSerializer);
            indexBlockCache.add(position, block);
        }
        return block;
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
        IndexEntry<K> entryKey = entrySerializer.fromBytes(entry);
        entry.position(prevPos);
        return entryKey.key.compareTo(key);
    }


    public Metrics metrics() {
        return metrics;
    }

    void writeTo(FooterWriter writer) {
        //write index blocks
        int blockSize = Math.min(bufferPool.bufferSize(), Size.KB.ofInt(4));

        Block block = blockFactory.create(blockSize);

        this.bloomFilter = BloomFilter.create(items.size(), bloomFilterFP);

        for (IndexEntry<K> kv : items) {
            addToBloomFilter(kv.key);
            if (!block.add(kv, entrySerializer, bufferPool)) {
                long blockPos = writer.write(block, blockSerializer);
                addToMidpoints(blockPos, block);
                block.clear();
                block.add(kv, entrySerializer, bufferPool);
            }
        }

        if (!block.isEmpty()) {
            long blockPos = writer.write(block, blockSerializer);
            addToMidpoints(blockPos, block);
        }
        items = null;

        midpoints.writeTo(writer, codec, bufferPool, keySerializer);
        bloomFilter.writeTo(writer, codec, bufferPool);
        readOnly = true;
    }

    private void addToMidpoints(long position, Block block) {
        IndexEntry<K> first = entrySerializer.fromBytes(block.first());
        IndexEntry<K> last = entrySerializer.fromBytes(block.last());

        Midpoint<K> start = new Midpoint<>(first.key, position);
        Midpoint<K> end = new Midpoint<>(last.key, position);

        midpoints.add(start, end);
    }

    public K firstKey() {
        return midpoints.first().key;
    }

    public K lastKey() {
        return midpoints.last().key;
    }

    public IndexEntry<K> first() {
        return getAt(midpoints.first(), true);
    }

    public IndexEntry<K> last() {
        return getAt(midpoints.last(), false);
    }

    //firstLast is a hacky way of getting either the first or last block element
    //true if first block element
    //false if last block element
    private IndexEntry<K> getAt(Midpoint<K> midpoint, boolean firstLast) {
        Block block = reader.read(midpoint.position, blockSerializer);
        ByteBuffer lastEntry = firstLast ? block.first() : block.last();
        return entrySerializer.fromBytes(lastEntry);
    }

    public boolean isEmpty() {
        return midpoints.isEmpty();
    }

    long startPos(Direction direction, Range<K> range) {
        if (Direction.FORWARD.equals(direction)) {
            Midpoint<K> mStart = range.start() == null ? midpoints.first() : midpoints.getMidpointFor(range.start());
            return mStart == null ? midpoints.first().position : mStart.position;
        }
        Midpoint<K> mEnd = range.end() == null ? midpoints.last() : midpoints.getMidpointFor(range.end());
        return mEnd == null ? midpoints.last().position : mEnd.position;

    }

    private Long blockPos(K key) {
        Midpoint<K> midpoint = midpoints.getMidpointFor(key);
        return midpoint == null ? null : midpoint.position;
    }

    @Override
    public IndexEntry<K> floor(K key) {
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
            Block block = readBlock(midpoint.position);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 2 : entryIdx;
            IndexEntry<K> found = readNextNonExpired(block, entryIdx, Direction.BACKWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    public IndexEntry<K> ceiling(K key) {
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
            Block block = readBlock(midpoint.position);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 1 : entryIdx;
            IndexEntry<K> found = readNextNonExpired(block, entryIdx, Direction.FORWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    public IndexEntry<K> higher(K key) {
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
            Block block = readBlock(midpoint.position);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 1 : entryIdx + 1;
            IndexEntry<K> found = readNextNonExpired(block, entryIdx, Direction.FORWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    @Override
    public IndexEntry<K> lower(K key) {
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
            Block block = readBlock(midpoint.position);
            int entryIdx = binarySearch(block, key);
            entryIdx = entryIdx < 0 ? Math.abs(entryIdx) - 2 : entryIdx - 1;
            IndexEntry<K> found = readNextNonExpired(block, entryIdx, Direction.BACKWARD);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private IndexEntry<K> readNextNonExpired(Block block, int idx, Direction direction) {
        while (idx >= 0 && idx < block.entryCount()) {
            IndexEntry<K> found = readNonExpired(block, idx);
            if (found != null) {
                return found;
            }
            idx = Direction.FORWARD.equals(direction) ? idx + 1 : idx - 1;
        }
        return null;
    }

    private IndexEntry<K> readNonExpired(Block block, int i) {
        ByteBuffer data = block.get(i);
        IndexEntry<K> entry = entrySerializer.fromBytes(data);
        return entry.readable(maxAge) ? entry : null;
    }


    private static class IndexEntrySerializer<K extends Comparable<K>> implements Serializer<IndexEntry<K>> {

        private final Serializer<K> keySerializer;

        private IndexEntrySerializer(Serializer<K> keySerializer) {
            this.keySerializer = keySerializer;
        }

        @Override
        public void writeTo(IndexEntry<K> data, ByteBuffer dst) {
            Serializers.LONG.writeTo(data.timestamp, dst);
            keySerializer.writeTo(data.key, dst);
            Serializers.LONG.writeTo(data.value, dst);
        }

        @Override
        public IndexEntry<K> fromBytes(ByteBuffer buffer) {
            long timestamp = Serializers.LONG.fromBytes(buffer);
            K key = keySerializer.fromBytes(buffer);
            long pos = Serializers.LONG.fromBytes(buffer);
            return new IndexEntry<>(timestamp, key, pos);
        }
    }

}
