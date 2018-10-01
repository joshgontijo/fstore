package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.State;

public class TaskResult {

    public final Status status;
    public final Failure failure;
    public final State state;
    public final Metrics metrics;

}
