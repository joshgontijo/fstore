package io.joshworks.fstore.index;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.midpoints.Midpoint;
import io.joshworks.fstore.index.midpoints.Midpoints;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * In memory IMMUTABLE, ORDERED sparse index.
 * All entries are kept in disk, FIRST and LAST entry of each block is kept in memory.
 * <p>
 * Format:
 * MIDPOINT BLOCK POS (8 bytes)
 * INDEX_ENTRY BLOCK 1 (n bytes)
 * INDEX_ENTRY BLOCK 2 (n bytes)
 * INDEX_ENTRY BLOCK N (n bytes)
 * MIDPOINTS BLOCK (n bytes)
 */
public class SparseIndex<K extends Comparable<K>> implements Index<K> {

    private static final Codec CODEC = Codec.noCompression();

    private final FooterReader reader;
    private final BlockFactory blockFactory;

    private final Serializer<K> keySerializer;
    private final int sparseness;
    private final IndexEntrySerializer<K> serializer;

    private final Midpoints<K> midpoints = new Midpoints<>();
    private final ConcurrentSkipListSet<IndexEntry<K>> memItems = new ConcurrentSkipListSet<>(Comparator.comparing(ie -> ie.key));

    //TODO add block cache

    public SparseIndex(Serializer<K> keySerializer, int sparseness, FooterReader reader, BlockFactory blockFactory) {
        this.serializer = new IndexEntrySerializer<>(keySerializer);
        this.keySerializer = keySerializer;
        this.sparseness = sparseness;
        this.reader = reader;
        this.blockFactory = blockFactory;
    }

    public void load() {
        Long midpointBlockPos = reader.read(Serializers.LONG);
        ByteBuffer blockData = reader.read(midpointBlockPos, Serializers.COPY);
        midpoints.deserialize(blockData, keySerializer);
    }

    @Override
    public IndexEntry<K> get(K entry) {
        new IndexEntry<>(entry, -1);
        if (!memItems.isEmpty()) {
            IndexEntry<K> identity = IndexEntry.identity(entry);
            IndexEntry<K> found = memItems.floor(IndexEntry.identity(entry));
            if (found == null) {
                return null;
            }
            return identity.compareTo(entry) == 0 ? found : null;
        }
        Midpoint<K> midpoint = midpoints.getMidpointFor(entry);
        List<IndexEntry<K>> entries = loadEntries(midpoint);
        int idx = Collections.binarySearch(entries, entry);
        if (idx < 0) {
            return null;
        }
        return entries.get(idx);
    }

    @Override
    public void add(K key, long pos) {
        Objects.requireNonNull(key, "Index key cannot be null");
        memItems.add(new IndexEntry<>(key, pos));
    }

    @Override
    public void writeTo(FooterWriter writer) {
        long startPos = writer.position();
        writer.position(writer.position() + Long.BYTES + RecordHeader.HEADER_OVERHEAD);

        Iterator<IndexEntry<K>> iterator = memItems.iterator();
        Block block = blockFactory.create(Integer.MAX_VALUE);

        long i = 0;
        IndexEntry<K> firstEntry = null;
        IndexEntry<K> lastEntry = null;
        while (iterator.hasNext()) {
            IndexEntry<K> entry = iterator.next();
            firstEntry = firstEntry == null ? entry : firstEntry;
            lastEntry = entry;
            if (!block.add(serializer.toBytes(entry))) {
                throw new IllegalStateException("No block space");
            }

            if (++i % sparseness == 0) {
                ByteBuffer packed = block.pack(CODEC);
                long blockPos = writer.write(packed);
                block = blockFactory.create(Integer.MAX_VALUE);
                addToMidpoint(firstEntry, lastEntry, blockPos);
                firstEntry = null;
                lastEntry = null;
            }
        }

        if (!block.isEmpty()) {
            ByteBuffer packed = block.pack(CODEC);
            long blockPos = writer.write(packed);
            if (firstEntry == null) {
                throw new IllegalStateException("Midpoints must not be null");
            }
            addToMidpoint(firstEntry, lastEntry, blockPos);
        }

        ByteBuffer midpointData = midpoints.serialize(keySerializer);
        long pos = writer.write(midpointData);
        long endPos = writer.position();
        writer.position(startPos);
        writer.write(Serializers.LONG.toBytes(pos));
        writer.position(endPos);
    }

    private void addToMidpoint(IndexEntry<K> first, IndexEntry<K> last, long blockPos) {
        Midpoint<K> start = new Midpoint<>(first.key, blockPos);
        Midpoint<K> end = new Midpoint<>(last.key, blockPos);
        midpoints.add(start, end);
    }

    private Block loadBlock(long pos) {
        ByteBuffer data = reader.read(pos, Serializers.COPY);
        return blockFactory.load(CODEC, data);
    }

    private List<IndexEntry<K>> loadEntries(Midpoint<K> midpoint) {
        if (midpoint == null) {
            return Collections.emptyList();
        }
        Block block = loadBlock(midpoint.position);
        return block.deserialize(serializer);
    }

//    public Iterator<IndexEntry<K>> range(K startInclusive, K endExclusive) {
//        Midpoint<K> low = midpoints.getMidpointFor(startInclusive);
//        Midpoint<K> high = midpoints.getMidpointFor(endExclusive);
//
//        if (low != null) {
//            List<IndexEntry<K>> indexEntries = loadEntries(low);
//        }
//
//
//        return map.subMap(startInclusive, endExclusive).entrySet().stream().map(kv -> new IndexEntry<>(kv.getKey(), kv.getValue())).collect(Collectors.toList());
//    }

    @Override
    public Iterator<IndexEntry<K>> iterator() {
        return null;
    }

    private class SparseIndexIterator implements Iterator<IndexEntry<K>> {

        private final Queue<IndexEntry<K>> entries = new ArrayDeque<>();
        private boolean empty;
        private final Midpoint<K> start;
        private final Midpoint<K> end;

        private SparseIndexIterator(Midpoint<K> start, Midpoint<K> end) {
            this.start = start;
            this.end = end;
        }


        private void fetchEntries() {
            if (empty) {
                return;
            }
            ByteBuffer blockData = reader.read(Serializers.COPY);
            if (!blockData.hasRemaining() && !empty) {
                empty = true;
                return;
            }
            Block block = blockFactory.load(CODEC, blockData);
            entries.addAll(block.deserialize(serializer));
        }

        @Override
        public boolean hasNext() {
            if (entries.isEmpty()) {
                fetchEntries();
            }
            return entries.isEmpty();
        }

        @Override
        public IndexEntry<K> next() {
            if (!hasNext()) {
                return null;
            }
            return entries.poll();
        }
    }

}
