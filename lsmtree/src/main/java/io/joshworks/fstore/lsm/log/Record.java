package io.joshworks.fstore.lsm.log;

import io.joshworks.fstore.lsm.EntryType;

public class Record<K, V> {

    public final EntryType type;
    public final K key;
    public final V value;

    private Record(EntryType type, K key, V value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public static <K, V> Record<K, V> add(K key, V value) {
        return new Record<>(EntryType.ADD, key, value);
    }

    public static <K, V> Record<K, V> delete(K key) {
        return new Record<>(EntryType.DELETE, key, null);
    }

    static <K, V> Record<K, V> memFlushed() {
        return new Record<>(EntryType.MEM_FLUSHED, null, null);
    }

}
