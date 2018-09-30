package io.joshworks.eventry.projections;

import java.util.Map;

public class ExecutionResult {

    public final Status type;
    public final Projection projection;
    public final Failure failure;
    public final State state;
    public final Map<String, Object> options;
    public final Metrics metrics;

    private ExecutionResult(Projection projection, Status status, State state, Metrics metrics, Map<String, Object> options, Failure failure) {
        this.projection = projection;
        this.type = status;
        this.state = state;
        this.metrics = metrics;
        this.options = options;
        this.failure = failure;
    }

    public static ExecutionResult completed(Projection projection, ProjectionContext context, Metrics metrics) {
        return new ExecutionResult(projection, Status.COMPLETED, context.state(), metrics, context.options, null);
    }

    public static ExecutionResult failed(Projection projection, ProjectionContext context, Metrics metrics, Exception ex, String stream, int version) {
        return new ExecutionResult(projection, Status.FAILED, context.state(), metrics, context.options, new Failure(ex.getMessage(), metrics.logPosition, stream, version));
    }

    public static ExecutionResult stopped(Projection projection, ProjectionContext context, Metrics metrics) {
        return new ExecutionResult(projection, Status.STOPPED, context.state(), metrics, context.options, null);
    }

    public static class Failure {
        public final String reason;
        public final long logPosition;
        public final String stream;
        public final int version;

        public Failure(String reason, long logPosition, String stream, int version) {
            this.reason = reason;
            this.logPosition = logPosition;
            this.stream = stream;
            this.version = version;
        }
    }

}
