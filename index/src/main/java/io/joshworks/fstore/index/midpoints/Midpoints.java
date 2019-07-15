package io.joshworks.fstore.index.midpoints;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.VLenBlock;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class Midpoints<K extends Comparable<K>> {

    private final List<Midpoint<K>> entries = new ArrayList<>();

    public void add(Midpoint<K> start, Midpoint<K> end) {
        if (!entries.isEmpty()) {
            entries.remove(entries.size() - 1);
        }
        entries.add(start);
        entries.add(end);
    }

    //returns -1 if no floor item or the floor index of this item
    public int floorIdx(K key) {
        if (entries.isEmpty()) {
            return -1;
        }
        if (key.compareTo(first().key) < 0) {
            return -1;
        }
        if (inRange(key)) {
            return getMidpointIdx(key);
        }
        return entries.size() - 1;
    }

    public int ceilingIdx(K key) {
        if (entries.isEmpty()) {
            return -1;
        }
        if (key.compareTo(last().key) > 0) {
            return -1;
        }
        if (inRange(key)) {
            return getMidpointIdx(key);
        }
        return 0;
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
            int idx = Collections.binarySearch(entries, key);
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
        int idx = Collections.binarySearch(entries, entry);
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

    public ByteBuffer serialize(Serializer<K> keySerializer) {
        Serializer<Midpoint<K>> serializer = new MidpointSerializer<>(keySerializer);

        Block midpointsBlock = VLenBlock.factory().create(Integer.MAX_VALUE);
        for (Midpoint<K> midpoint : entries) {
            ByteBuffer data = serializer.toBytes(midpoint);
            if (!midpointsBlock.add(data)) {
                throw new IllegalStateException("No block space");
            }
        }
        entries.sort(Comparator.comparing(o -> o.key));
        return midpointsBlock.pack(Codec.noCompression());
    }

    public static <K extends Comparable<K>> Midpoints<K> load(ByteBuffer blockData, Serializer<K> keySerializer) {
        Midpoints<K> midpoints = new Midpoints<>();
        if (!midpoints.isEmpty()) {
            throw new IllegalStateException("Midpoints is not empty");
        }

        Serializer<Midpoint<K>> serializer = new MidpointSerializer<>(keySerializer);
        Block block = VLenBlock.factory().load(Codec.noCompression(), blockData);
        List<Midpoint<K>> entries = block.deserialize(serializer);
        midpoints.entries.addAll(entries);
        midpoints.entries.sort(Comparator.comparing(o -> o.key));

        return midpoints;
    }
}
