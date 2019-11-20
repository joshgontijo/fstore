package io.joshworks.fstore.projection;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.projection.result.ScriptExecutionResult;
import io.joshworks.fstore.projection.task.StopReason;

import java.util.List;

public interface EventStreamHandler {

    ScriptExecutionResult onStart(State state);

    ScriptExecutionResult onStop(StopReason reason, State state);

    void onEvent(JsonEvent record, State state);

    State aggregateState(State first, State second);

    ScriptExecutionResult processEvents(List<EventRecord> events, State state) throws ScriptExecutionException;

}
