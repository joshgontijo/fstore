package io.joshworks.eventry.query;

import com.sun.jdi.event.EventIterator;
import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.JsonEvent;
import io.joshworks.eventry.projections.State;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

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

//    @Override
//    public boolean test(JsonEvent record) {
//        try {
//            Object result = invocable.invokeFunction(FUNCTION_NAME, record.stream, record.version, record.data, record.type, record.timestamp, record.metadata);
//            return result instanceof Boolean && (Boolean) result;
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }

    public State query(State state) {
        try {
            while (iterator.hasNext()) {
                JsonEvent record = JsonEvent.from(iterator.next());
                invocable.invokeFunction(FUNCTION_NAME, state, record.stream, record.version, record.data, record.type, record.timestamp, record.metadata);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return state;
    }
}
