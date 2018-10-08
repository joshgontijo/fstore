package io.joshworks.eventry.projections.result;

public enum Status {
    RUNNING(1),
    STOPPED(2),
    COMPLETED(4),
    FAILED(8);

    public int flag;

    Status(int flag) {
        this.flag = flag;
    }
}
