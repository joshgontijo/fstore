package io.joshworks.fstore.index.cache;

public interface Cache<K, V> {

    void add(K key, V newValue);

    V get(K key);

    V remove(K key);

    void clear();

    static <K, V> Cache<K, V> create(int size, int maxAge) {
        return size > 0 ? new LRUCache<>(size, maxAge) : new NoCache<>();
    }

}
