package io.joshworks.fstore.index.midpoints;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.VLenBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Midpoints<K extends Comparable<K>> {

    private final List<Midpoint<K>> midpoints = new ArrayList<>();

    public void add(Midpoint<K> start, Midpoint<K> end) {
        if (midpoints.isEmpty()) {
            midpoints.add(start);
            midpoints.add(end);
        } else {
            midpoints.set(midpoints.size() - 1, start);
            midpoints.add(end);
        }
    }

    public int getMidpointIdx(K entry) {
        if (!inRange(entry)) {
            return -1;
        }
        int idx = Collections.binarySearch(midpoints, entry);
        if (idx < 0) {
            idx = Math.abs(idx) - 2; // -1 for the actual position, -1 for the offset where to start scanning
            idx = idx < 0 ? 0 : idx;
        }
        if (idx >= midpoints.size()) {
            throw new IllegalStateException("Got index " + idx + " midpoints position: " + midpoints.size());
        }
        return idx;
    }

    public Midpoint<K> getMidpointFor(K entry) {
        int midpointIdx = getMidpointIdx(entry);
        if (midpointIdx >= midpoints.size() || midpointIdx < 0) {
            return null;
        }
        return midpoints.get(midpointIdx);
    }

    public boolean inRange(K entry) {
        if (midpoints.isEmpty()) {
            return false;
        }
        return entry.compareTo(first().key) >= 0 && entry.compareTo(last().key) <= 0;
    }

    public int size() {
        return midpoints.size();
    }

    public boolean isEmpty() {
        return midpoints.isEmpty();
    }

    public Midpoint<K> first() {
        if (midpoints.isEmpty()) {
            return null;
        }
        return midpoints.get(0);
    }

    public Midpoint<K> last() {
        if (midpoints.isEmpty()) {
            return null;
        }
        return midpoints.get(midpoints.size() - 1);
    }

    public ByteBuffer serialize(Serializer<K> keySerializer) {
        Serializer<Midpoint<K>> serializer = new MidpointSerializer<>(keySerializer);

        Block midpointsBlock = VLenBlock.factory().create(Integer.MAX_VALUE);
        for (Midpoint<K> midpoint : midpoints) {
            ByteBuffer data = serializer.toBytes(midpoint);
            if (!midpointsBlock.add(data)) {
                throw new IllegalStateException("No block space");
            }
        }
        return midpointsBlock.pack(Codec.noCompression());
    }

    public void deserialize(ByteBuffer blockData, Serializer<K> keySerializer) {
        if (!midpoints.isEmpty()) {
            throw new IllegalStateException("Midpoints is not empty");
        }

        Serializer<Midpoint<K>> serializer = new MidpointSerializer<>(keySerializer);
        Block block = VLenBlock.factory().load(Codec.noCompression(), blockData);
        List<Midpoint<K>> entries = block.deserialize(serializer);
        midpoints.addAll(entries);
    }


}
