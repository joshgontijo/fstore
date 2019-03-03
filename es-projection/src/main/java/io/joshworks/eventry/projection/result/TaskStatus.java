package io.joshworks.eventry.projection.result;

public class TaskStatus {

    public final Status status;
    public final TaskError taskError;
    public final Metrics metrics;

    public TaskStatus(Status status, TaskError taskError, Metrics metrics) {
        this.status = status;
        this.taskError = taskError;
        this.metrics = metrics;
    }

}
