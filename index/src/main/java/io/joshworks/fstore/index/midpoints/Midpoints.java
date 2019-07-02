package io.joshworks.fstore.index.midpoints;

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


}
