package io.joshworks.eventry;

import io.joshworks.eventry.projections.JsonEvent;

public class ScriptExecutionException extends Exception {

    public final JsonEvent event;

    public ScriptExecutionException(Throwable e, JsonEvent event) {
        super(e);
        this.event = event;
    }
}
