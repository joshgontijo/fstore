package io.joshworks.eventry.projections.result;

import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.ProjectionContext;
import io.joshworks.eventry.projections.State;

import java.util.Map;
import java.util.Set;

public class ExecutionResult {

    public final Map<String, Object> options;
    public final Set<String> sources;
    public final Status status;
    public final Projection projection;
    public final Failure failure;
    public final State state;
    public final Metrics metrics;

    private ExecutionResult(Projection projection, Set<String> sources, Status status, State state, Metrics metrics, Map<String, Object> options, Failure failure) {
        this.projection = projection;
        this.status = status;
        this.state = state;
        this.metrics = metrics;
        this.options = options;
        this.failure = failure;
    }

    public static ExecutionResult completed(Projection projection, Set<String> sources, ProjectionContext context, Metrics metrics) {
        return new ExecutionResult(projection, Status.COMPLETED, context.state(), metrics, context.options, null);
    }

    public static ExecutionResult failed(Projection projection, Set<String> sources, ProjectionContext context, Metrics metrics, Exception ex, String stream, int version) {
        return new ExecutionResult(projection, Status.FAILED, context.state(), metrics, context.options, new Failure(ex.getMessage(), metrics.logPosition, stream, version));
    }

    public static ExecutionResult stopped(Projection projection, Set<String> sources, ProjectionContext context, Metrics metrics) {
        return new ExecutionResult(projection, Status.STOPPED, context.state(), metrics, context.options, null);
    }

}
