package io.joshworks.eventry.projection.query;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.projection.JsonEvent;
import io.joshworks.eventry.projection.State;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

public class Jsr223QueryPredicate {

    private final Invocable invocable;
    private static final String ENGINE_NAME = "nashorn";
    private static final String FUNCTION_NAME = "_query";
    private final EventLogIterator iterator;

    public Jsr223QueryPredicate(String script, EventLogIterator iterator) {
        this.iterator = iterator;
        try {
            ScriptEngine engine = new ScriptEngineManager().getEngineByName(ENGINE_NAME);

            String template = "function _query(state, stream, version, body, type, timestamp, metadata) { %s }";
            String result = String.format(template, script);

            engine.eval(result);

            this.invocable = (Invocable) engine;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public State query(State state) {
        try {
            while (iterator.hasNext()) {
                JsonEvent record = JsonEvent.from(iterator.next());
                invocable.invokeFunction(FUNCTION_NAME, state, record.stream, record.version, record.body, record.type, record.timestamp, record.metadata);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return state;
    }
}
