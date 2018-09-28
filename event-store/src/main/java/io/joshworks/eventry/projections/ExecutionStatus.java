package io.joshworks.eventry.projections;

public class ExecutionStatus {

    public final State state;
    public final String stream;
    public final int version;
    public final long processedItems;
    public final long processedTime;

    public ExecutionStatus(State state, String stream, int version, long processedItems, long processedTime) {
        this.state = state;
        this.stream = stream;
        this.version = version;
        this.processedItems = processedItems;
        this.processedTime = processedTime;
    }

    public enum State {
        STARTING,
        RUNNING,
        FAILED,
        STOPPED,
        COMPLETED,
        UNKNOWN
    }
}
