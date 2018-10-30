package io.joshworks.eventry;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int maxCacheSize;

    public LRUCache(int maxCacheSize) {
        super(maxCacheSize, 0.75F, true);
        this.maxCacheSize = maxCacheSize;
    }

    public static <K, V> Map<K, V> synchronizedCache(int maxCacheSize) {
        return Collections.synchronizedMap(new LRUCache<>(maxCacheSize));
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return this.size() > this.maxCacheSize;
    }
}