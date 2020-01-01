package io.joshworks.ilog;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

class IndexEntry<K extends Comparable<K>> implements Comparable<K> {

    final K key;
    final long logPosition;

    IndexEntry(K key, long logPosition) {
        this.key = requireNonNull(key);
        this.logPosition = logPosition;
    }

    @Override
    public int compareTo(K o) {
        return key.compareTo(o);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexEntry<?> that = (IndexEntry<?>) o;
        return logPosition == that.logPosition &&
                key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, logPosition);
    }

    @Override
    public String toString() {
        return "IndexEntry{" +
                "key=" + key +
                ", logPosition=" + logPosition +
                '}';
    }
}
