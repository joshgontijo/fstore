package io.joshworks.eventry.projections.meta;

public abstract class JavaHandler implements EventStreamHandler {

    protected ProjectionContext context;

    public JavaHandler(ProjectionContext context) {
        this.context = context;
    }

}
