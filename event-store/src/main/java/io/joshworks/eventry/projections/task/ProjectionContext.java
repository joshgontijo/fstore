package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectionContext {

    private final IEventStore store;
    private final State state = new State();
    private final Map<String, Object> options = new HashMap<>();

    public ProjectionContext(IEventStore store) {
        this.store = store;
    }

    public void publishEvents(List<ScriptExecutionResult.OutputEvent> outputs) {
        for (var output : outputs) {
            output.handle(store);
        }
    }

    public final State state() {
        return state;
    }

    public final void options(Map<String, Object> options) {
        this.options.putAll(options);
    }

    public final void initialState(Map<String, Object> initialState) {
        if (initialState != null) {
            this.state.putAll(initialState);
        }
    }

    public Map<String, Object> options() {
        return new HashMap<>(options);
    }

}