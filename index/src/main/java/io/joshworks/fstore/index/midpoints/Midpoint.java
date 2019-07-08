package io.joshworks.fstore.index.midpoints;

import java.util.Objects;

public class Midpoint<K extends Comparable<K>> implements Comparable<K> {

    public final K key;
    public final int blockHash;

    public Midpoint(K key, int blockHash) {
        this.key = key;
        this.blockHash = blockHash;
    }

    @Override
    public int compareTo(K o) {
        return key.compareTo(o);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Midpoint midpoint = (Midpoint) o;
        return blockHash == midpoint.blockHash &&
                Objects.equals(key, midpoint.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, blockHash);
    }
}
