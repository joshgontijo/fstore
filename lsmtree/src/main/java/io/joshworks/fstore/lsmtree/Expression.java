package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.lsmtree.sstable.Entry;

public enum Expression {
    FLOOR, CEILING, HIGHER, LOWER;


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
            default:
                throw new IllegalArgumentException("Invalid Expression");
        }
    }

}
