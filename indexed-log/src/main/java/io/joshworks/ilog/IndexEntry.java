package io.joshworks.ilog;

class IndexEntry<K extends Comparable<K>> implements Comparable<K> {

    final K key;
    final long value;

    IndexEntry(K key, long value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(K o) {
        return key.compareTo(o);
    }
}
