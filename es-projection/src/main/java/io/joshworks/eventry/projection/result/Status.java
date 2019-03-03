package io.joshworks.eventry.projection.result;

public enum Status {
    NOT_STARTED(1 << 0), //not yet started
    AWAITING(1 << 1),//awaiting for data
    RUNNING(1 << 2), //started and has data
    STOPPED(1 << 3), //stopped either by the user or the system
    COMPLETED(1 << 4), //no more entries to process, never the case for CONTINUOUS types
    FAILED(1 << 5); // failed

    public int flag;

    Status(int flag) {
        this.flag = flag;
    }
}
