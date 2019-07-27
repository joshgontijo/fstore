package io.joshworks.fstore.core.cache;

public class NoCache<K, V> implements Cache<K, V> {

    @Override
    public void add(K key, V newValue) {

    }

    @Override
    public V get(K key) {
        return null;
    }

    @Override
    public V remove(K key) {
        return null;
    }

    @Override
    public void clear() {

    }
}
