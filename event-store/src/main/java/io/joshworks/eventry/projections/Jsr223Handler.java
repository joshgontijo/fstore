package io.joshworks.eventry.projections;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Jsr223Handler implements EventStreamHandler {

    private static final String ON_EVENT_METHOD_NAME = "onEvent";
    private static final String FILTER_METHOD_NAME = "filter";
    private static final String SOURCE_METHOD_NAME = "source";
    private static final String OPTIONS_METHOD_NAME = "options";

    private static final String SOURCE_STREAMS_FIELD_NAME = "streams";
    private static final String SOURCE_PARALLEL_FIELD_NAME = "parallel";

    private static final String EMIT_METHOD_NAME = "emit";
    private static final String LINK_TO_METHOD_NAME = "linkTo";

    private final Invocable invocable;
    private final StreamSource source;

    public Jsr223Handler(ProjectionContext ctx, String script, String engineName) {
        try {
            BiConsumer<String, JsonEvent> emmit = ctx::emit;
            BiConsumer<String, JsonEvent> linkTo = ctx::linkTo;
            Consumer<Map<String, Object>> options = ctx::options;
            Consumer<Map<String, Object>> source = ctx::source;

            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
            engine.put(OPTIONS_METHOD_NAME, options);
            engine.put(SOURCE_METHOD_NAME, source);
            engine.put(EMIT_METHOD_NAME, emmit);
            engine.put(LINK_TO_METHOD_NAME, linkTo);

            engine.eval(script);
            this.invocable = (Invocable) engine;
            this.source = getStreamSource(ctx);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private StreamSource getStreamSource(ProjectionContext ctx) {
        try {
            List<String> streams = (List<String>) ctx.source().get(SOURCE_STREAMS_FIELD_NAME);
            Boolean parallel = (Boolean) ctx.source().get(SOURCE_PARALLEL_FIELD_NAME);
            return new StreamSource(new HashSet<>(streams), parallel != null && parallel);
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

    @Override
    public StreamSource source() {
        return source;
    }

}
