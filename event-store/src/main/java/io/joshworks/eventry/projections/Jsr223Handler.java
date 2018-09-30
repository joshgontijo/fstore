package io.joshworks.eventry.projections;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Jsr223Handler implements EventStreamHandler {

    private static final String ON_EVENT_METHOD_NAME = "onEvent";
    private static final String FILTER_METHOD_NAME = "filter";
    private static final String OPTIONS_METHOD_NAME = "options";
    private static final String STATE = "state";
    private static final String EMIT_METHOD_NAME = "emit";
    private static final String LINK_TO_METHOD_NAME = "linkTo";

    private final Invocable invocable;

    public Jsr223Handler(ProjectionContext ctx, String script, String engineName) {
        try {
            BiConsumer<String, JsonEvent> emmit = ctx::emit;
            BiConsumer<String, JsonEvent> linkTo = ctx::linkTo;
            Consumer<Map<String, Object>> options = ctx::options;

            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
            engine.put(OPTIONS_METHOD_NAME, options);
//            engine.put(STATE, ctx.state());
            engine.put(EMIT_METHOD_NAME, emmit);
            engine.put(LINK_TO_METHOD_NAME, linkTo);

            engine.eval(script);
            this.invocable = (Invocable) engine;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean filter(JsonEvent record, State state) {
        try {
            Boolean result = (Boolean) invocable.invokeFunction(FILTER_METHOD_NAME, record, state);
            return result != null && result;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onEvent(JsonEvent record, State state) {
        try {
            invocable.invokeFunction(ON_EVENT_METHOD_NAME, record, state);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
