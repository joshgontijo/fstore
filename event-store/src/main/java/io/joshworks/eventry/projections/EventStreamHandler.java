package io.joshworks.eventry.projections;

import io.joshworks.eventry.ScriptExecutionException;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;

import java.util.List;

public interface EventStreamHandler {

    void onEvent(JsonEvent record, State state);

    State aggregateState(State first, State second);

    ScriptExecutionResult processEvents(List<EventRecord> events, State state) throws ScriptExecutionException;

}
