package io.joshworks.fstore.core.cache;

import java.util.Map;

class SoftCache<K, V> implements Cache<K, V> {

    private final Map<K, V> map = new SoftHashMap<>();

    @Override
    public void add(K key, V newValue) {
        map.put(key, newValue);
    }

    @Override
    public V get(K key) {
        return map.get(key);
    }

    @Override
    public V remove(K key) {
        return map.remove(key);
    }

    @Override
    public void clear() {
        map.clear();
    }
}
