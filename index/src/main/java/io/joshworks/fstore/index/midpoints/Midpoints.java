package io.joshworks.fstore.index.midpoints;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;
import io.joshworks.fstore.log.segment.block.BlockSerializer;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Midpoints<K extends Comparable<K>> {

    private static final String BLOCK_PREFIX = "MIDPOINT_";

    private final List<Midpoint<K>> entries = new ArrayList<>();

    public void add(Midpoint<K> start, Midpoint<K> end) {
        if (!entries.isEmpty()) {
            entries.remove(entries.size() - 1);
        }
        entries.add(start);
        entries.add(end);
    }

    public Midpoint<K> lower(K key) {
        if (entries.isEmpty()) {
            return null;
        }
        if (key.compareTo(first().key) <= 0) {
            return null;
        }
        int idx = binarySearch(key);
        idx = idx < 0 ? Math.abs(idx) - 2 : idx - 1;
        if (idx < 0) {
            return null;
        }
        return entries.get(idx);
    }

    public int binarySearch(K key) {
        return Collections.binarySearch(entries, key);
    }

    public Midpoint<K> floor(K key) {
        if (entries.isEmpty()) {
            return null;
        }
        if (key.compareTo(first().key) < 0) {
            return null;
        }
        if (inRange(key)) {
            return getMidpointFor(key);
        }

        //greater than last midpoint
        return entries.get(entries.size() - 1);
    }

    public Midpoint<K> ceiling(K key) {
        if (entries.isEmpty()) {
            return null;
        }
        if (key.compareTo(last().key) > 0) {
            return null;
        }
        if (inRange(key)) {
            int idx = binarySearch(key);
            idx = idx < 0 ? Math.abs(idx) - 2 : idx + 1;
            return getMidpoint(idx);
        }

        //less than first midpoint
        return entries.get(0);
    }

    public int getMidpointIdx(K entry) {
        if (!inRange(entry)) {
            return -1;
        }
        int idx = binarySearch(entry);
        return idx < 0 ? Math.abs(idx) - 2 : idx;
    }

    public Midpoint<K> getMidpointFor(K entry) {
        int midpointIdx = getMidpointIdx(entry);
        return getMidpoint(midpointIdx);
    }

    public Midpoint<K> getMidpoint(int midpointIdx) {
        if (midpointIdx >= entries.size() || midpointIdx < 0) {
            return null;
        }
        return entries.get(midpointIdx);
    }

    public boolean inRange(K entry) {
        if (entries.isEmpty()) {
            return false;
        }
        return entry.compareTo(first().key) >= 0 && entry.compareTo(last().key) <= 0;
    }

    public int size() {
        return entries.size();
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public Midpoint<K> first() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(0);
    }

    public Midpoint<K> last() {
        if (entries.isEmpty()) {
            return null;
        }
        return entries.get(entries.size() - 1);
    }

    public void writeTo(FooterWriter writer, Codec codec, BufferPool bufferPool, Serializer<K> keySerializer) {
        Serializer<Midpoint<K>> serializer = new MidpointSerializer<>(keySerializer);

        int blockSize = Math.min(bufferPool.bufferSize(), Size.MB.ofInt(1));
        BlockFactory blockFactory = Block.vlenBlock();
        BlockSerializer blockSerializer = new BlockSerializer(codec, blockFactory);
        Block block = blockFactory.create(blockSize);

        int blockIdx = 0;
        entries.sort(Comparator.comparing(o -> o.key));
        for (Midpoint<K> midpoint : entries) {
            if (!block.add(midpoint, serializer, bufferPool)) {
                writer.write(BLOCK_PREFIX + blockIdx, block, blockSerializer);
                block.clear();
                blockIdx++;
            }
        }

        if (!block.isEmpty()) {
            writer.write(BLOCK_PREFIX + blockIdx, block, blockSerializer);
        }
    }

    public static <K extends Comparable<K>> Midpoints<K> load(FooterReader reader, Codec codec, Serializer<K> keySerializer) {
        Midpoints<K> midpoints = new Midpoints<>();
        if (!midpoints.isEmpty()) {
            throw new IllegalStateException("Midpoints is not empty");
        }

        BlockFactory blockFactory = Block.vlenBlock();
        BlockSerializer blockSerializer = new BlockSerializer(codec, blockFactory);
        Serializer<Midpoint<K>> midpointSerializer = new MidpointSerializer<>(keySerializer);
        List<Block> blocks = reader.findAll(BLOCK_PREFIX, blockSerializer);

        for (Block block : blocks) {
            List<Midpoint<K>> entries = block.deserialize(midpointSerializer);
            midpoints.entries.addAll(entries);
        }
        midpoints.entries.sort(Comparator.comparing(o -> o.key));
        return midpoints;
    }
}
