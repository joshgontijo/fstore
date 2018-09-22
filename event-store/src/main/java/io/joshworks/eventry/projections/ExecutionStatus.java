package io.joshworks.eventry.projections;

public class ExecutionStatus {

    public final State state;
    public final String stream;
    public final int version;
    public final long processedItems;

    public ExecutionStatus(State state, String stream, int version, long processedItems) {
        this.state = state;
        this.stream = stream;
        this.version = version;
        this.processedItems = processedItems;
    }

    public enum State {
        RUNNING,
        FAILED,
        STOPPED,
        COMPLETED,
        UNKNOWN
    }
}
