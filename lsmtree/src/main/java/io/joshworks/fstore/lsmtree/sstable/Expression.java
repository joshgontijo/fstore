package io.joshworks.fstore.lsmtree.sstable;

public enum Expression {
    FLOOR, CEILING, HIGHER, LOWER, EQUALS;


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
