package io.joshworks.fstore.projection;


public class ScriptExecutionException extends Exception {

    public final JsonEvent event;

    public ScriptExecutionException(Throwable e, JsonEvent event) {
        super(e);
        this.event = event;
    }
}
