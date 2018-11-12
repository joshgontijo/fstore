package io.joshworks.eventry.projections.result;

public enum Status {
    NOT_STARTED(1 << 0),
    AWAITING(1 << 1),
    RUNNING(1 << 2),
    STOPPED(1 << 3),
    COMPLETED(1 << 4),
    FAILED(1 << 5);

    public int flag;

    Status(int flag) {
        this.flag = flag;
    }
}
