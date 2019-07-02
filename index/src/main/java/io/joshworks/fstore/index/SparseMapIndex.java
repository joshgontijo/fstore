package io.joshworks.fstore.index;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.index.midpoints.Midpoint;
import io.joshworks.fstore.index.midpoints.Midpoints;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
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
 * In memory ORDERED sparse index. All entries are kept in disk, FIRST and LAST entry of each block is kept in memory.
 * <p>
 * Format:
 * MIDPOINTS BLOCK POS (8bytes)
 * INDEX_ENTRY BLOCK 1 (n bytes)
 * INDEX_ENTRY BLOCK 2 (n bytes)
 * INDEX_ENTRY BLOCK N (n bytes)
 * MIDPOINTS BLOCK (n bytes)
 */
public class SparseMapIndex<K extends Comparable<K>> implements Index<K> {

    private static final Codec CODEC = Codec.noCompression();

    private static final Long NONE = -1L;
    private final int blockSize;

    private final List<Block> blocks = new ArrayList<>();
    private final BlockFactory blockFactory;
    private final FooterReader reader;

    private final IndexEntrySerializer<K> serializer;
    private final Midpoints<K> midpoints = new Midpoints<>();

    private Block block;

    //TODO add bock cache

    public SparseMapIndex(Serializer<K> keySerializer, int blockSize, FooterReader reader, BlockFactory blockFactory) {
        this.serializer = new IndexEntrySerializer<>(keySerializer);
        this.blockSize = blockSize;
        this.reader = reader;
        this.blockFactory = blockFactory;
        this.block = blockFactory.create(blockSize);
    }

    @Override
    public void add(K key, long pos) {
        Objects.requireNonNull(key, "Index key cannot be null");
        ByteBuffer data = serializer.toBytes(new IndexEntry<>(key, pos));
        while (!block.add(data)) {
            blocks.add(block);
            block = blockFactory.create(blockSize);
        }
    }

    @Override
    public void writeTo(FooterWriter writer) {
        long startPos = writer.position();
        writer.skip(Long.BYTES);

        Block midpointsBlock = VLenBlock.factory().create(Integer.MAX_VALUE);

        for (Block block : blocks) {
            ByteBuffer packed = block.pack(CODEC);
            long blockPos = writer.write(packed);
            ByteBuffer[] entries = addToMidpoint(block, blockPos);
            for (ByteBuffer midpointEntry : entries) {
                if (!midpointsBlock.add(midpointEntry)) {
                    throw new IllegalStateException("No block space");
                }
            }
        }

        long pos = writer.write(midpointsBlock.pack(Codec.noCompression()));
        writer.position(startPos);
        writer.write(Serializers.LONG.toBytes(pos));
        blocks.clear();
    }

    private ByteBuffer[] addToMidpoint(Block block, long blockPos) {
        ByteBuffer firstData = block.first();
        ByteBuffer lastData = block.last();

        IndexEntry<K> first = serializer.fromBytes(firstData);
        IndexEntry<K> last = serializer.fromBytes(lastData);

        Midpoint<K> start = new Midpoint<>(first.key, blockPos);
        Midpoint<K> end = new Midpoint<>(last.key, blockPos);
        midpoints.add(start, end);

        return new ByteBuffer[]{serializeMidpoint(firstData, blockPos), serializeMidpoint(lastData, blockPos)};
    }

    private ByteBuffer serializeMidpoint(ByteBuffer key, long blockPos) {
        return ByteBuffer.allocate(key.remaining() + Long.BYTES)
                .put(key)
                .putLong(blockPos)
                .flip();
    }


    private Block loadBlock(long pos) {
        ByteBuffer data = reader.read(pos, Serializers.NONE);
        return blockFactory.load(CODEC, data);
    }

    private List<IndexEntry<K>> loadEntries(Midpoint<K> midpoint) {
        if (midpoint == null) {
            return Collections.emptyList();
        }
        Block block = loadBlock(midpoint.position);
        return block.deserialize(serializer);
    }

    private TreeMap<K, Long> load(ByteBuffer blockData) {
        if (blockData == null) {
            return new TreeMap<>();
        }

        Block block = VLenBlock.factory().load(Codec.noCompression(), blockData);
        List<IndexEntry<K>> entries = block.entries().stream().map(serializer::fromBytes).collect(Collectors.toList());
        TreeMap<K, Long> map = new TreeMap<>();
        for (IndexEntry<K> entry : entries) {
            map.put(entry.key, entry.position);
        }
        return map;

    }

    @Override
    public long get(K entry) {
        Midpoint<K> midpoint = midpoints.getMidpointFor(entry);
        List<IndexEntry<K>> entries = loadEntries(midpoint);
        int idx = Collections.binarySearch(entries, entry);
        if (idx < 0) {
            return -1;
        }
        return entries.get(idx).position;
    }

    public boolean inRange(K entry) {
        return midpoints.inRange(entry);
    }

    public Iterator<IndexEntry<K>> range(K startInclusive, K endExclusive) {
        Midpoint<K> low = midpoints.getMidpointFor(startInclusive);
        Midpoint<K> high = midpoints.getMidpointFor(endExclusive);

        if(low != null) {
            List<IndexEntry<K>> indexEntries = loadEntries(low);
        }


        return map.subMap(startInclusive, endExclusive).entrySet().stream().map(kv -> new IndexEntry<>(kv.getKey(), kv.getValue())).collect(Collectors.toList());
    }

    @Override
    public Iterator<IndexEntry<K>> iterator() {
        return null;
    }
}
