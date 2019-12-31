package io.joshworks.ilog;

class IndexEntry<K extends Comparable<K>> implements Comparable<K> {

    final K key;
    final long logPosition;

    IndexEntry(K key, long logPosition) {
        this.key = key;
        this.logPosition = logPosition;
    }

    @Override
    public int compareTo(K o) {
        return key.compareTo(o);
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "key=" + key +
                ", logPosition=" + logPosition +
                '}';
    }
}
