package io.joshworks.eventry.projections;

public interface EventStreamHandler {

    boolean filter(JsonEvent record, State state);

    void onEvent(JsonEvent record, State state);

    StreamSource source();

}
