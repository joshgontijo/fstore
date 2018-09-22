package io.joshworks.eventry.projections;

public class StopRequest extends RuntimeException {

    public StopRequest() {
        super("Stop request");
    }
}
