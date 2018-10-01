package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.ProjectionContext;
import io.joshworks.eventry.projections.State;

import java.util.Map;
import java.util.Set;

public class ExecutionResult {

    public final String projectionName;
    public final Map<String, Object> options;
    public final Set<String> sources;
    public final Status status;
    public final Failure failure;
    public final State state;
    public final Metrics metrics;

    private ExecutionResult(String projectionName, Set<String> sources, Status status, State state, Metrics metrics, Map<String, Object> options, Failure failure) {
        this.projectionName = projectionName;
        this.sources = sources;
        this.status = status;
        this.state = state;
        this.metrics = metrics;
        this.options = options;
        this.failure = failure;
    }

    public static ExecutionResult completed(String projectionName, Set<String> sources, ProjectionContext context, Metrics metrics) {
        return new ExecutionResult(projectionName, sources, Status.COMPLETED, context.state(), metrics, context.options(), null);
    }

    public static ExecutionResult failed(String projectionName, Set<String> sources, ProjectionContext context, Metrics metrics, Exception ex, String stream, int version) {
        return new ExecutionResult(projectionName, sources, Status.FAILED, context.state(), metrics, context.options(), new Failure(ex.getMessage(), metrics.logPosition, stream, version));
    }

    public static ExecutionResult stopped(String projectionName, Set<String> sources, ProjectionContext context, Metrics metrics) {
        return new ExecutionResult(projectionName, sources, Status.STOPPED, context.state(), metrics, context.options(), null);
    }

}
