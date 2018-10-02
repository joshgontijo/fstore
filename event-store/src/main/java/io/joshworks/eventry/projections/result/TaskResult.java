package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.State;

public class TaskResult {

    public final String stream;
    public final Status status;
    public final Failure failure;
    public final State state;
    public final Metrics metrics;

    private TaskResult(String stream, Status status, Failure failure, State state, Metrics metrics) {
        this.stream = stream;
        this.status = status;
        this.failure = failure;
        this.state = state;
        this.metrics = metrics;
    }

    public static TaskResult completed(String projectionName, State state, Metrics metrics) {
        return new TaskResult(projectionName, Status.COMPLETED, null, state, metrics);
    }

    public static TaskResult failed(String projectionName, State state, Metrics metrics, Exception ex, String stream, int version) {
        return new TaskResult(projectionName, Status.FAILED, new Failure(ex.getMessage(), metrics.logPosition, stream, version), state, metrics);
    }

    public static TaskResult stopped(String projectionName, State state, Metrics metrics) {
        return new TaskResult(projectionName, Status.STOPPED, null, state, metrics);
    }

}
