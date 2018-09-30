package io.joshworks.eventry.projections.meta;

import io.joshworks.eventry.projections.JsonEvent;

public interface EventStreamHandler {

    boolean filter(JsonEvent record, State state);

    void onEvent(JsonEvent record, State state);

}
