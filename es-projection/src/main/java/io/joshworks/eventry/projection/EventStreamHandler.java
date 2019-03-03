package io.joshworks.eventry.projection;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projection.result.ScriptExecutionResult;
import io.joshworks.eventry.projection.task.StopReason;

import java.util.List;

public interface EventStreamHandler {

    ScriptExecutionResult onStart(State state);

    ScriptExecutionResult onStop(StopReason reason, State state);

    void onEvent(JsonEvent record, State state);

    State aggregateState(State first, State second);

    ScriptExecutionResult processEvents(List<EventRecord> events, State state) throws ScriptExecutionException;

}
