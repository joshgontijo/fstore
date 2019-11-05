//package io.joshworks.fstore.index;
//
//import io.joshworks.fstore.codec.snappy.SnappyCodec;
//import io.joshworks.fstore.core.Codec;
//import io.joshworks.fstore.core.Serializer;
//import io.joshworks.fstore.index.cache.LRUCache;
//import io.joshworks.fstore.index.filter.BloomFilter;
//import io.joshworks.fstore.index.midpoints.Midpoint;
//import io.joshworks.fstore.index.midpoints.Midpoints;
//import io.joshworks.fstore.log.segment.block.Block;
//import io.joshworks.fstore.log.segment.block.BlockFactory;
//import io.joshworks.fstore.log.segment.block.VLenBlock;
//import io.joshworks.fstore.log.segment.footer.FooterReader;
//import io.joshworks.fstore.log.segment.footer.FooterWriter;
//import io.joshworks.fstore.serializer.Serializers;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayDeque;
//import java.util.Collections;
//import java.util.Comparator;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Objects;
//import java.util.Queue;
//import java.util.concurrent.ConcurrentSkipListSet;
//
///**
// * In memory IMMUTABLE, ORDERED non clustered sparse index.
// * Keys are kept in the footer, data values is kept in the data area which is not responsability of this class
// * All entries are kept in disk, FIRST and LAST entry of each block are kept in memory.
// * <p>
// * A bloom filter is used to avoid unnecessary disk access.
// *
// * <p>
// * Format:
// * MIDPOINT BLOCK POS (8 bytes)
// * BLOOM_FILTER BLOCK POS (8 bytes)
// * INDEX_ENTRY BLOCK 1 (n bytes)
// * INDEX_ENTRY BLOCK 2 (n bytes)
// * INDEX_ENTRY BLOCK N (n bytes)
// * MIDPOINTS BLOCK (n bytes)
// */
//public class NonClusteredSparseIndex<K extends Comparable<K>> implements Index<K> {
//
//    private static final String MIDPOINT_BLOCK = "MIDPOINT";
//    private static final String BLOOM_FILTER_BLOCK = "BLOOM_FILTER";
//    private static final String INDEX_BLOCK_PREFIX = "SPARSE_INDEX_";
//
//    private final FooterReader reader;
//    private final BlockFactory blockFactory;
//
//    private final double bfFalseNegativeProb;
//    private BloomFilter<K> filter;
//
//    private final Serializer<K> keySerializer;
//    private final Codec codec;
//    private final int sparseness;
//    private final IndexEntrySerializer<K> serializer;
//
//    private final Midpoints<K> midpoints = new Midpoints<>();
//    private final ConcurrentSkipListSet<IndexEntry<K>> memItems = new ConcurrentSkipListSet<>(Comparator.comparing(ie -> ie.key));
//
//    private final LRUCache<Long, Block> blockCache;
//
//    private NonClusteredSparseIndex(
//            Serializer<K> keySerializer,
//            Codec codec,
//            int sparseness,
//            double bfFalseNegativeProb,
//            int blockCacheSize,
//            int blockCacheMaxAge,
//            FooterReader reader,
//            BlockFactory blockFactory) {
//        this.serializer = new IndexEntrySerializer<>(keySerializer);
//        this.blockCache = blockCacheSize > 0 ? new LRUCache<>(blockCacheSize, blockCacheMaxAge) : null;
//        this.keySerializer = keySerializer;
//        this.codec = codec;
//        this.sparseness = sparseness;
//        this.bfFalseNegativeProb = bfFalseNegativeProb;
//        this.reader = reader;
//        this.blockFactory = blockFactory;
//        this.load();
//    }
//
//    public static <K extends Comparable<K>> Builder<K> builder(Serializer<K> serializer, FooterReader reader) {
//        return new Builder<>(serializer, reader);
//    }
//
//    private void load() {
//        ByteBuffer blockData = reader.read(MIDPOINT_BLOCK, Serializers.COPY);
//        if (blockData != null) {
//            this.midpoints.load(blockData, keySerializer);
//        }
//
//        ByteBuffer bfData = reader.read(BLOOM_FILTER_BLOCK, Serializers.COPY);
//        if (blockData != null) {
//            this.filter = BloomFilter.load(bfData, keySerializer);
//        }
//    }
//
//    @Override
//    public IndexEntry<K> get(K entry) {
//        new IndexEntry<>(entry, -1);
//        if (!memItems.isEmpty()) {
//            return getFromMemory(entry);
//        }
//        return fromDisk(entry);
//    }
//
//    private IndexEntry<K> fromDisk(K entry) {
//        if (filter != null && !filter.contains(entry)) {
//            return null;
//        }
//        Midpoint<K> midpoint = midpoints.getMidpointFor(entry);
//        List<IndexEntry<K>> entries = loadEntries(midpoint);
//        int idx = Collections.binarySearch(entries, entry);
//        if (idx < 0) {
//            return null;
//        }
//        return entries.get(idx);
//    }
//
//    private IndexEntry<K> getFromMemory(K entry) {
//        IndexEntry<K> identity = IndexEntry.identity(entry);
//        IndexEntry<K> found = memItems.floor(IndexEntry.identity(entry));
//        if (found == null) {
//            return null;
//        }
//        return identity.compareTo(entry) == 0 ? found : null;
//    }
//
//    @Override
//    public void add(K key, long pos) {
//        Objects.requireNonNull(key, "Index key cannot be null");
//        memItems.add(new IndexEntry<>(key, pos));
//    }
//
//    @Override
//    public void writeTo(FooterWriter writer) {
//        filter = BloomFilter.create(memItems.size(), bfFalseNegativeProb, keySerializer);
//
//        Iterator<IndexEntry<K>> iterator = memItems.iterator();
//        Block block = blockFactory.create(Integer.MAX_VALUE);
//
//        long i = 0;
//        int writtenBlocks = 0;
//        IndexEntry<K> firstEntry = null;
//        IndexEntry<K> lastEntry = null;
//        while (iterator.hasNext()) {
//            IndexEntry<K> entry = iterator.next();
//            filter.add(entry.key);
//
//            firstEntry = firstEntry == null ? entry : firstEntry;
//            lastEntry = entry;
//            if (!block.add(serializer.toBytes(entry))) {
//                throw new IllegalStateException("No block space");
//            }
//
//            if (++i % sparseness == 0) {
//                ByteBuffer packed = block.pack(codec);
//                String footerItemName = getBlockName(writtenBlocks++);
//                int blockHash = writer.write(footerItemName, packed);
//                block = blockFactory.create(Integer.MAX_VALUE);
//                addToMidpoint(firstEntry, lastEntry, blockHash);
//                firstEntry = null;
//                lastEntry = null;
//            }
//        }
//
//        if (!block.isEmpty()) {
//            ByteBuffer packed = block.pack(codec);
//            String footerItemName = getBlockName(writtenBlocks);
//            int blockHash = writer.write(footerItemName, packed);
//            if (firstEntry == null) {
//                throw new IllegalStateException("Midpoints must not be null");
//            }
//            addToMidpoint(firstEntry, lastEntry, blockHash);
//        }
//
//        ByteBuffer midpointData = midpoints.serialize(keySerializer);
//        writer.write(MIDPOINT_BLOCK, midpointData);
//
//        ByteBuffer bloomFilterData = filter.serialize();
//        writer.write(BLOOM_FILTER_BLOCK, bloomFilterData);
//    }
//
//    private String getBlockName(int blockIdx) {
//        return INDEX_BLOCK_PREFIX + blockIdx;
//    }
//
//    private void addToMidpoint(IndexEntry<K> first, IndexEntry<K> last, long position) {
//        Midpoint<K> start = new Midpoint<>(first.key, position);
//        Midpoint<K> end = new Midpoint<>(last.key, position);
//        midpoints.add(start, end);
//    }
//
//    private Block loadBlock(long position) {
//        if (blockCache != null) {
//            Block block = blockCache.get(position);
//            if (block != null) {
//                return block;
//            }
//        }
//        ByteBuffer data = reader.read(position, Serializers.COPY);
//        Block loaded = blockFactory.load(codec, data);
//        if (blockCache != null) {
//            blockCache.add(blockHash, loaded);
//        }
//        return loaded;
//    }
//
//    private List<IndexEntry<K>> loadEntries(Midpoint<K> midpoint) {
//        if (midpoint == null) {
//            return Collections.emptyList();
//        }
//        Block block = loadBlock(midpoint.position);
//        return block.deserialize(serializer);
//    }
//
////    public Iterator<IndexEntry<K>> range(K startInclusive, K endExclusive) {
////        Midpoint<K> low = midpoints.getMidpointFor(startInclusive);
////        Midpoint<K> high = midpoints.getMidpointFor(endExclusive);
////
////        if (low != null) {
////            List<IndexEntry<K>> indexEntries = loadEntries(low);
////        }
////
////
////        return map.subMap(startInclusive, endExclusive).entrySet().stream().map(kv -> new IndexEntry<>(kv.getKey(), kv.getValue())).collect(Collectors.toList());
////    }
//
//    @Override
//    public Iterator<IndexEntry<K>> iterator() {
//        return null;
//    }
//
//
//    //    public SparseIndex(Serializer<K> keySerializer, Codec codec, int sparseness, double bfFalseNegativeProb, FooterReader reader, BlockFactory blockFactory) {
////        this.serializer = new IndexEntrySerializer<>(keySerializer);
////        this.keySerializer = keySerializer;
////        this.codec = codec;
////        this.sparseness = sparseness;
////        this.bfFalseNegativeProb = bfFalseNegativeProb;
////        this.reader = reader;
////        this.blockFactory = blockFactory;
////    }
//    public static class Builder<K extends Comparable<K>> {
//
//        private final Serializer<K> keySerializer;
//        private final FooterReader reader;
//        private Codec codec = new SnappyCodec();
//        private int sparseness = 256;
//        private double falsePositiveProb = 0.01;
//        private BlockFactory blockFactory = VLenBlock.factory();
//        private int blockCacheSize = 10;
//        private int blockCacheMaxAge = -1;
//
//        private Builder(Serializer<K> keySerializer, FooterReader reader) {
//            this.keySerializer = keySerializer;
//            this.reader = reader;
//        }
//
//
//        public Builder<K> codec(Codec codec) {
//            this.codec = codec;
//            return this;
//        }
//
//        public Builder<K> sparseness(int sparseness) {
//            this.sparseness = sparseness;
//            return this;
//        }
//
//        //-1 disabled
//        public Builder<K> blockCacheMaxsize(int blockCacheSize) {
//            this.blockCacheSize = blockCacheSize;
//            return this;
//        }
//
//        public Builder<K> blockCacheMaxAge(int blockCacheMaxAge) {
//            this.blockCacheMaxAge = blockCacheMaxAge;
//            return this;
//        }
//
//        public Builder<K> bloomFilterFalsePositive(double falsePositiveProb) {
//            this.falsePositiveProb = falsePositiveProb;
//            return this;
//        }
//
//        public Builder<K> blockFactory(BlockFactory blockFactory) {
//            this.blockFactory = blockFactory;
//            return this;
//        }
//
//        public NonClusteredSparseIndex<K> build() {
//            return new NonClusteredSparseIndex<>(keySerializer, codec, sparseness, falsePositiveProb, blockCacheSize, blockCacheMaxAge, reader, blockFactory);
//        }
//
//    }
//
//    private class SparseIndexIterator implements Iterator<IndexEntry<K>> {
//
//        private final Queue<IndexEntry<K>> entries = new ArrayDeque<>();
//        private boolean empty;
//        private final Midpoint<K> start;
//        private final Midpoint<K> end;
//        private int blocksRead;
//
//        private SparseIndexIterator(Midpoint<K> start, Midpoint<K> end) {
//            this.start = start;
//            this.end = end;
//        }
//
//
//        private void fetchEntries() {
//            if (empty) {
//                return;
//            }
//            String footerItemName = getBlockName(blocksRead);
//            ByteBuffer blockData = reader.read(footerItemName, Serializers.COPY);
//            if (!blockData.hasRemaining() && !empty) {
//                empty = true;
//                return;
//            }
//            blocksRead++;
//            Block block = blockFactory.load(codec, blockData);
//            entries.addAll(block.deserialize(serializer));
//        }
//
//        @Override
//        public boolean hasNext() {
//            if (entries.isEmpty()) {
//                fetchEntries();
//            }
//            return entries.isEmpty();
//        }
//
//        @Override
//        public IndexEntry<K> next() {
//            if (!hasNext()) {
//                return null;
//            }
//            return entries.poll();
//        }
//    }
//
//}
