//package io.joshworks.fstore.index;
//
//import io.joshworks.fstore.core.Codec;
//import io.joshworks.fstore.core.Serializer;
//import io.joshworks.fstore.index.filter.BloomFilter;
//import io.joshworks.fstore.log.segment.block.Block;
//import io.joshworks.fstore.log.segment.block.VLenBlock;
//import io.joshworks.fstore.log.segment.footer.FooterReader;
//import io.joshworks.fstore.log.segment.footer.FooterWriter;
//import io.joshworks.fstore.serializer.Serializers;
//
//import java.nio.ByteBuffer;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Objects;
//import java.util.TreeMap;
//import java.util.stream.Collectors;
//
///**
// * Red black tree based index, all entries are kept in disk.
// * Data is written to file on segment roll
// */
//public class MapIndex<K extends Comparable<K>> implements Index<K> {
//
//    private static final Long NONE = -1L;
//    private final TreeMap<K, Long> memItems;
//    private final boolean readonly;
//
//    private static final String BLOOM_FILTER_BLOCK = "BLOOM_FILTER";
//
//    private final IndexEntrySerializer<K> serializer;
//    private BloomFilter<K> filter;
//
//    public MapIndex(Serializer<K> keySerializer, FooterReader reader) {
//        serializer = new IndexEntrySerializer<>(keySerializer);
//        this.memItems = load(reader);
//        this.readonly = !memItems.isEmpty();
//    }
//
//    @Override
//    public void add(K key, long pos) {
//        if (readonly) {
//            throw new IllegalStateException("Index is read only");
//        }
//        Objects.requireNonNull(key, "Index key cannot be null");
//        memItems.put(key, pos);
//    }
//
//    @Override
//    public IndexEntry<K> get(K entry) {
//        Long pos = memItems.getOrDefault(entry, NONE);
//        return pos >= 0 ? new IndexEntry<>(entry, pos) : null;
//    }
//
//    @Override
//    public void writeTo(FooterWriter writer) {
//        filter = BloomFilter.create(memItems.size(), bfFalseNegativeProb, keySerializer);
//
//        Block block = new VLenBlock(Integer.MAX_VALUE);
//
//        Iterator<Map.Entry<K, Long>> iterator = memItems.entrySet().iterator();
//        while (iterator.hasNext()) {
//            Map.Entry<K, Long> entry = iterator.next();
//            ByteBuffer data = serializer.toBytes(new IndexEntry<>(entry.getKey(), entry.getValue()));
//            if (!block.add(data)) {
//                throw new IllegalStateException("No block space available");
//            }
//            iterator.remove();
//        }
//
//        ByteBuffer blockData = block.pack(Codec.noCompression());
//        writer.write(blockData);
//    }
//
//
//    private TreeMap<K, Long> load(FooterReader reader) {
//        ByteBuffer blockData = reader.read(Serializers.COPY);
//        Block block = VLenBlock.factory().load(Codec.noCompression(), blockData);
//        List<IndexEntry<K>> entries = block.entries().stream().map(serializer::fromBytes).collect(Collectors.toList());
//        TreeMap<K, Long> map = new TreeMap<>();
//        for (IndexEntry<K> entry : entries) {
//            map.put(entry.key, entry.position);
//        }
//        return map;
//
//    }
//
//    public int size() {
//        return memItems.size();
//    }
//
//    public boolean isEmpty() {
//        return memItems.isEmpty();
//    }
//
//    public IndexEntry<K> first() {
//        Map.Entry<K, Long> entry = memItems.firstEntry();
//        if (entry == null) {
//            return null;
//        }
//        return new IndexEntry<>(entry.getKey(), entry.getValue());
//    }
//
//
//    public IndexEntry last() {
//        Map.Entry<K, Long> entry = memItems.lastEntry();
//        if (entry == null) {
//            return null;
//        }
//        return new IndexEntry<>(entry.getKey(), entry.getValue());
//    }
//
//    //TODO no thread safe
//    public List<IndexEntry<K>> range(K startInclusive, K endExclusive) {
//        return memItems.subMap(startInclusive, endExclusive).entrySet().stream().map(kv -> new IndexEntry<>(kv.getKey(), kv.getValue())).collect(Collectors.toList());
//    }
//
//
//    @Override
//    public Iterator<IndexEntry<K>> iterator() {
//        return null;
//    }
//}
