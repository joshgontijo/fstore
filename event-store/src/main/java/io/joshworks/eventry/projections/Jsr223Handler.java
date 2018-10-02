package io.joshworks.eventry.projections;

import io.joshworks.eventry.utils.StringUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class Jsr223Handler implements EventStreamHandler {

    private static final String ON_EVENT_METHOD_NAME = "onEvent";
    private static final String FILTER_METHOD_NAME = "filter";
    private static final String CONFIG_METHOD_NAME = "config";
    private static final String INITIAL_STATE_METHOD_NAME = "state";

    private static final String SOURCE_STREAMS_FIELD_NAME = "streams";
    private static final String SOURCE_PARALLEL_FIELD_NAME = "parallel";
    private static final String ENABLED_FIELD_NAME = "enabled";
    private static final String PROJECTION_NAME_FIELD_NAME = "name";
    private static final String TYPE_NAME_FIELD_NAME = "type";

    private static final String EMIT_METHOD_NAME = "emit";
    private static final String LINK_TO_METHOD_NAME = "linkTo";

    private final Invocable invocable;
    private final StreamSource source;

    public Jsr223Handler(ProjectionContext ctx, String script, String engineName) {
        try {
            BiConsumer<String, JsonEvent> emmit = ctx::emit;
            BiConsumer<String, JsonEvent> linkTo = ctx::linkTo;
            Consumer<Map<String, Object>> config = ctx::options;
            Consumer<Map<String, Object>> initialState = ctx::initialState;

            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
            engine.put(CONFIG_METHOD_NAME, config);
            engine.put(INITIAL_STATE_METHOD_NAME, initialState);

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
            Map<String, String> sourceStreams = (Map<String, String>) ctx.options().get(SOURCE_STREAMS_FIELD_NAME);
            Boolean parallel = (Boolean) ctx.options().get(SOURCE_PARALLEL_FIELD_NAME);
            return new StreamSource(new HashSet<>(sourceStreams.values()), parallel != null && parallel);
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

    public static Projection compile(String script, String engineName) {
        try {
            Map<String, Object> options = new HashMap<>();
            Map<String, Object> initialState = new HashMap<>();
            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
            Consumer<Map<String, Object>> configConsumer = options::putAll;
            Consumer<Map<String, Object>> initialStateConsumer = initialState::putAll;
            engine.put(CONFIG_METHOD_NAME, configConsumer);
            engine.put(INITIAL_STATE_METHOD_NAME, initialStateConsumer);

            engine.eval(script);

            Object sourceStreams = options.get(SOURCE_STREAMS_FIELD_NAME);
            Object projectionName = options.get(PROJECTION_NAME_FIELD_NAME);
            Object type = options.get(TYPE_NAME_FIELD_NAME);
            Object parallel = options.get(SOURCE_PARALLEL_FIELD_NAME);
            Object enabled = options.get(ENABLED_FIELD_NAME);

            if (sourceStreams == null) {
                throw new RuntimeException("No source stream provided");
            }
            Set<String> streams = new HashSet<>(((Map<String, String>) sourceStreams).values());
            streams.removeIf(String::isEmpty);
            if (streams.isEmpty()) {
                throw new RuntimeException("No source stream provided");
            }

            if (projectionName == null || StringUtils.isBlank(String.valueOf(projectionName))) {
                throw new RuntimeException("No projection name provided");
            }
            if (type == null || StringUtils.isBlank(String.valueOf(type))) {
                throw new RuntimeException("No source stream provided");
            }
            Projection.Type theType;
            try {
                theType = Projection.Type.valueOf(String.valueOf(type));
            } catch (Exception e) {
                throw new RuntimeException("Invalid projection type '" + type + "'");
            }

            boolean isParallel = parallel != null && ((Boolean) parallel);
            boolean isEnabled = enabled == null || ((Boolean) enabled);

            return new Projection(script, String.valueOf(projectionName), streams, theType, isEnabled, isParallel);

        } catch (Exception e) {
            throw new RuntimeException("Script compilation error: " + e.getMessage(), e);
        }
    }

}
