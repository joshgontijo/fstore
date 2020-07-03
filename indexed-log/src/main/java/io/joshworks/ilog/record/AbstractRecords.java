package io.joshworks.ilog.record;

abstract class AbstractRecords implements Records {

    private final String poolName;

    AbstractRecords(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public String poolName() {
        return poolName;
    }
}
