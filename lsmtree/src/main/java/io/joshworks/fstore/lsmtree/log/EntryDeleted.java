package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

import java.util.Objects;

public class EntryDeleted<K> extends LogRecord {

    public final K key;

    public EntryDeleted(long timestamp, K key) {
        super(EntryType.DELETE, timestamp);
        this.key = key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        EntryDeleted<?> that = (EntryDeleted<?>) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key);
    }
}
