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
import java.util.Objects;
import java.util.stream.Collectors;

public class Index<K extends Comparable<K>> {

    private final List<IndexEntry<K>> entries;
    private final IndexEntrySerializer<K> serializer;

    public Index(Serializer<K> keySerializer, FooterReader reader) {
        serializer = new IndexEntrySerializer<>(keySerializer);
        this.entries = load(reader);
    }

    public void add(K key, long pos) {
        Objects.requireNonNull(key, "Index key cannot be null");
        this.entries.add(new IndexEntry<>(key, pos));
    }

    public void write(FooterWriter writer) {
        Block block = new VLenBlock(Integer.MAX_VALUE);

        Iterator<IndexEntry<K>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            IndexEntry<K> indexEntry = iterator.next();
            if (!block.add(serializer.toBytes(indexEntry))) {
                throw new IllegalStateException("No block space available");
            }
            iterator.remove();
        }

        ByteBuffer blockData = block.pack(Codec.noCompression());
        writer.write(blockData);
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
