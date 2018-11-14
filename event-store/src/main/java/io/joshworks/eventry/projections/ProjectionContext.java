package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProjectionContext {

    private final IEventStore store;
    private final Projection projection;
    private final State state = new State();
    private final Map<String, Object> options = new HashMap<>();

    public ProjectionContext(IEventStore store, Projection projection) {
        this.store = store;
        this.projection = projection;
    }

    public void publishEvents(List<ScriptExecutionResult.OutputEvent> outputs) {
        for (var output : outputs) {
            output.handle(store);
        }
    }

//    public final void linkTo(String stream, JsonEvent record) {
//        store.linkTo(stream, record.stream, record.version, record.type);
//    }
//
//    public final void emit(String stream, JsonEvent record) {
//        store.emit(stream, record.toEvent());
//    }

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
