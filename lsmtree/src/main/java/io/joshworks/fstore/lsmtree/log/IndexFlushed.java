package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

import java.util.Objects;

class IndexFlushed extends LogRecord {

    final String token;

    IndexFlushed(long timestamp, String token) {
        super(EntryType.MEM_FLUSHED, timestamp);
        this.token = token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        IndexFlushed that = (IndexFlushed) o;
        return token.equals(that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), token);
    }
}
