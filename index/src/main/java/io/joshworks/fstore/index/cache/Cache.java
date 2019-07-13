package io.joshworks.fstore.index.cache;

public interface Cache<K, V> {

    void add(K key, V newValue);

    V get(K key);

    V remove(K key);

    void clear();
}
