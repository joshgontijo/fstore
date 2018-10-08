package io.joshworks.eventry.projections;

public abstract class JavaHandler implements EventStreamHandler {

    protected ProjectionContext context;

    public JavaHandler(ProjectionContext context) {
        this.context = context;
    }

}
