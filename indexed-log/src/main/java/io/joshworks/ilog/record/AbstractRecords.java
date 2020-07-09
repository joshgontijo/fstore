package io.joshworks.ilog.record;

import java.io.Closeable;

abstract class AbstractRecords implements Closeable {

    protected final RecordPool pool;

    AbstractRecords(RecordPool pool) {
        this.pool = pool;
    }

}
