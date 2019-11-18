package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.lsmtree.sstable.entry.Entry;

import java.util.TreeSet;

public enum Expression {
    FLOOR, CEILING, HIGHER, LOWER, EQUALS;

    public <K extends Comparable<K>, V> Entry<K, V> apply(K key, TreeSet<Entry<K, V>> set) {
        Entry<K, V> entryKey = Entry.key(key);
        switch (this) {
            case FLOOR:
                return set.floor(entryKey);
            case CEILING:
                return set.ceiling(entryKey);
            case HIGHER:
                return set.higher(entryKey);
            case LOWER:
                return set.lower(entryKey);
            case EQUALS:
                return set.contains(entryKey) ? entryKey : null;
            default:
                throw new IllegalArgumentException("Invalid Expression");
        }
    }

    public <K extends Comparable<K>, V> Entry<K, V> apply(K key, TreeFunctions<K, V> functions) {
        switch (this) {
            case FLOOR:
                return functions.floor(key);
            case CEILING:
                return functions.ceiling(key);
            case HIGHER:
                return functions.higher(key);
            case LOWER:
                return functions.lower(key);
            case EQUALS:
                return functions.get(key);
            default:
                throw new IllegalArgumentException("Invalid Expression");
        }
    }

}
