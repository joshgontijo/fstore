package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;

import java.util.Map;

public class ProjectionContext {

    private final IEventStore store;
    private final State state = new State();
    protected Map<String, Object> options;


    public ProjectionContext(IEventStore store) {
        this.store = store;
    }

    public final void linkTo(String stream, JsonEvent record) {
        store.linkTo(stream, record.toEvent());
    }

    public final void emit(String stream, JsonEvent record) {
        store.emit(stream, record.toEvent());
    }

    public final State state() {
        return state;
    }

    public final void options(Map<String, Object> options) {
        this.options = options;
    }

}
