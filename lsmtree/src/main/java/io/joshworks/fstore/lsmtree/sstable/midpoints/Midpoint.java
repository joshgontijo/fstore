package io.joshworks.fstore.lsmtree.sstable.midpoints;

import java.util.Objects;

public class Midpoint<K extends Comparable<K>> implements Comparable<K> {

    public final K key;
    public final long position;

    public Midpoint(K key, long position) {
        this.key = key;
        this.position = position;
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
        return position == midpoint.position &&
                Objects.equals(key, midpoint.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, position);
    }

    @Override
    public String toString() {
        return "{" + "key=" + key +
                ", position=" + position +
                '}';
    }
}
