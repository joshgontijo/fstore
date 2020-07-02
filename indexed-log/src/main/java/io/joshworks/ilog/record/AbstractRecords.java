package io.joshworks.ilog.record;

public abstract class AbstractRecords implements Records {

    private final String poolName;

    protected AbstractRecords(String poolName) {
        this.poolName = poolName;
    }

    @Override
    public String poolName() {
        return poolName;
    }
}
