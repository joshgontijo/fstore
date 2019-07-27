package io.joshworks.fstore.core.cache;

public interface Cache<K, V> {

    void add(K key, V newValue);

    V get(K key);

    V remove(K key);

    void clear();

    static <K, V> Cache<K, V> lruCache(int size, int maxAge) {
        return size > 0 ? new LRUCache<>(size, maxAge) : new NoCache<>();
    }

    static <K, V> Cache<K, V> softCache() {
        return new SoftCache<>();
    }

    static <K, V> Cache<K, V> noCache() {
        return new NoCache<>();
    }
}
