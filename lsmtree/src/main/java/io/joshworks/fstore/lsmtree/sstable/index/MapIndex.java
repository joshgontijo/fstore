package io.joshworks.fstore.lsmtree.sstable.index;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Red black tree based index, all entries are kept in memory.
 * Data is written to file on segment roll
 */
public class MapIndex<K extends Comparable<K>> {

    private final Map<K, Long> map = new TreeMap<>();

    private final IndexEntrySerializer<K> serializer;
    private final Serializer<K> keySerializer;

    public MapIndex(Serializer<K> keySerializer, FooterReader reader) {
        serializer = new IndexEntrySerializer<>(keySerializer);
        this.map = load(reader);
    }

    public void add(K key, long pos) {
        Objects.requireNonNull(key, "Index key cannot be null");
        map.put(key, pos);
    }

    public void write(FooterWriter writer) {
        Block block = new VLenBlock(Integer.MAX_VALUE);

        Iterator<Map.Entry<K, Long>> iterator = map.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<K, Long> entry = iterator.next();

            ByteBuffer keyData = keySerializer.toBytes(entry.getKey());
            Serializers.LONG.toBytes(keySerializer);

            if (!block.add(serializer.toBytes(entry))) {
                throw new IllegalStateException("No block space available");
            }
            iterator.remove();
        }

        ByteBuffer blockData = block.pack(Codec.noCompression());
        writer.write(blockData, Serializers.NONE);
    }

    private List<IndexEntry<K>> load(FooterReader reader) {
        if (reader.length() == 0) {
            return new ArrayList<>();
        }

        ByteBuffer blockData = reader.read(Serializers.NONE);
        if (blockData == null) {
            return Collections.emptyList();
        }

        Block block = VLenBlock.factory().load(Codec.noCompression(), blockData);
        return block.entries().stream().map(serializer::fromBytes).collect(Collectors.toList());
    }

    private int getMidpointIdx(K entry) {
        int idx = Collections.binarySearch(entries, entry);
        if (idx < 0) {
            idx = Math.abs(idx) - 2; // -1 for the actual position, -1 for the offset where to start scanning
            idx = idx < 0 ? 0 : idx;
        }
        if (idx >= entries.size()) {
            throw new IllegalStateException("Got index " + idx + " index position: " + entries.size());
        }
        return idx;
    }

    public IndexEntry get(K entry) {
        int midpointIdx = getMidpointIdx(entry);
        if (midpointIdx >= entries.size() || midpointIdx < 0) {
            return null;
        }
        return entries.get(midpointIdx);
    }

    public int size() {
        return entries.size();
    }

    public K first() {
        if (entries.isEmpty()) {
            return null;
        }
        return firstMidpoint().key;
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public K last() {
        if (entries.isEmpty()) {
            return null;
        }
        return lastMidpoint().key;
    }

    private IndexEntry<K> firstMidpoint() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(0);
    }

    private IndexEntry<K> lastMidpoint() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }

}
