package io.joshworks.fstore.lsm.sstable.index;


import java.util.Objects;

public class IndexEntry<K extends Comparable<K>> implements Comparable<K>{

    public final K key;
    public final long position;

    IndexEntry(K key, long position) {
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
        IndexEntry indexEntry = (IndexEntry) o;
        return position == indexEntry.position &&
                Objects.equals(key, indexEntry.key);
    }

    @Override
    public int hashCode() {

        return Objects.hash(key, position);
    }
}
