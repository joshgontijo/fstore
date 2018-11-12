package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.State;

public class TaskStatus {

    public final Status status;
    public final TaskError taskError;
    public final State state;
    public final Metrics metrics;

    public TaskStatus(Status status, TaskError taskError, State state, Metrics metrics) {
        this.status = status;
        this.taskError = taskError;
        this.state = state;
        this.metrics = metrics;
    }

}
