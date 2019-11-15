package io.joshworks.fstore.lsmtree.log;

import java.util.Objects;

public class EntryAdded<K, V> extends LogRecord {

    public final K key;
    public final V value;

    public EntryAdded(long timestamp, K key, V value) {
        super(EntryType.ADD, timestamp);
        this.key = key;
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EntryAdded<?, ?> that = (EntryAdded<?, ?>) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, value);
    }
}
