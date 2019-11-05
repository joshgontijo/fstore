package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

import java.util.Objects;

class IndexFlushedStarted extends LogRecord {

    final String token;
    final long position;

    IndexFlushedStarted(long timestamp, long position, String token) {
        super(EntryType.MEM_FLUSH_STARTED, timestamp);
        this.position = position;
        this.token = token;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        IndexFlushedStarted that = (IndexFlushedStarted) o;
        return position == that.position &&
                token.equals(that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), token, position);
    }
}
