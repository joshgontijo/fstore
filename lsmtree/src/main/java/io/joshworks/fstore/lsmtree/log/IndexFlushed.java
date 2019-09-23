package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.lsmtree.EntryType;

public class IndexFlushed extends LogRecord {

    public final long position;

    public IndexFlushed(long timestamp, long position) {
        super(EntryType.MEM_FLUSHED, timestamp);
        this.position = position;
    }
}
