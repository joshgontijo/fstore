package io.joshworks.ilog.record;

abstract class AbstractRecords implements Records {

    protected final RecordPool pool;

    AbstractRecords(RecordPool pool) {
        this.pool = pool;
    }

}
