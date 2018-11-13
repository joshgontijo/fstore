package io.joshworks.eventry.projections;

import io.joshworks.eventry.ScriptExecutionException;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.io.IOUtils;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Jsr223Handler implements EventStreamHandler {

    private static final String BASE_PROCESS_EVENTS_METHOD_NAME = "process_events";

    private static final String ON_EVENT_METHOD_NAME = "onEvent";
    private static final String FILTER_METHOD_NAME = "filter";
    private static final String CONFIG_METHOD_NAME = "config";
    private static final String INITIAL_STATE_METHOD_NAME = "state";

    private static final String SOURCE_STREAMS_FIELD_NAME = "streams";
    private static final String SOURCE_PARALLEL_FIELD_NAME = "parallel";
    private static final String PROJECTION_NAME_FIELD_NAME = "name";
    private static final String TYPE_NAME_FIELD_NAME = "type";

//    private static final String EMIT_METHOD_NAME = "emit";
//    private static final String LINK_TO_METHOD_NAME = "linkTo";

    private final Invocable invocable;
    private final SourceOptions source;

    Jsr223Handler(ProjectionContext ctx, String script, String engineName) {
        try {
//            BiConsumer<String, JsonEvent> emmit = ctx::emit;
//            BiConsumer<String, JsonEvent> linkTo = ctx::linkTo;

            Consumer<Map<String, Object>> config = ctx::options;
            Consumer<Map<String, Object>> initialState = ctx::initialState;

            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
            engine.put(CONFIG_METHOD_NAME, config);
            engine.put(INITIAL_STATE_METHOD_NAME, initialState);

//            engine.put(EMIT_METHOD_NAME, emmit);
//            engine.put(LINK_TO_METHOD_NAME, linkTo);

            InputStream is = this.getClass().getClassLoader().getResourceAsStream("projections/base.js");
            String baseScript = IOUtils.toString(is);
            engine.eval(baseScript);
            engine.eval(script);

            this.invocable = (Invocable) engine;
            this.source = getStreamSource(ctx);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private SourceOptions getStreamSource(ProjectionContext ctx) {
        try {
            Map<String, String> sourceStreams = (Map<String, String>) ctx.options().get(SOURCE_STREAMS_FIELD_NAME);
            Boolean parallel = (Boolean) ctx.options().get(SOURCE_PARALLEL_FIELD_NAME);
            return new SourceOptions(new HashSet<>(sourceStreams.values()), parallel != null && parallel);
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
    public SourceOptions source() {
        return source;
    }

    @Override
    public ScriptExecutionResult processEvents(List<EventRecord> events, State state) throws ScriptExecutionException {
        Queue<JsonEvent> jsonEvents = null;
        try {
            jsonEvents = events.stream().map(JsonEvent::from).collect(Collectors.toCollection(ArrayDeque::new));
            Map<String, Object> result = (Map<String, Object>) invocable.invokeFunction(BASE_PROCESS_EVENTS_METHOD_NAME, jsonEvents, state);

            Collection<Object> values = result.values();
            List<Map<String, Object>> parsed = new ArrayList<>();
            for (Object value : values) {
                parsed.add((Map<String, Object>) value);
            }
            return new ScriptExecutionResult(parsed);
        } catch (Exception e) {
            JsonEvent failed = jsonEvents != null && !jsonEvents.isEmpty() ? jsonEvents.poll() : null;
           throw new ScriptExecutionException(e, failed);
        }
    }

    static Projection compile(String script, String engineName) {
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

            if (sourceStreams == null) {
                throw new ScriptException("No source stream provided");
            }
            Set<String> streams = new HashSet<>(((Map<String, String>) sourceStreams).values());
            streams.removeIf(String::isEmpty);
            if (streams.isEmpty()) {
                throw new ScriptException("No source stream provided");
            }

            if (projectionName == null || StringUtils.isBlank(String.valueOf(projectionName))) {
                throw new ScriptException("No projection name provided");
            }
            if (type == null || StringUtils.isBlank(String.valueOf(type))) {
                throw new ScriptException("No source stream provided");
            }
            Projection.Type theType = getType(type);

            boolean isParallel = parallel != null && ((Boolean) parallel);

            return new Projection(script, String.valueOf(projectionName), engineName, streams, theType, isParallel);

        } catch (Exception e) {
            throw new CompilationException("Script compilation error: " + e.getMessage(), e);
        }
    }

    private static Projection.Type getType(Object type) {
        try {
            return Projection.Type.valueOf(String.valueOf(type).trim().toUpperCase());
        } catch (Exception e) {
            throw new RuntimeException("Invalid projection type '" + type + "'");
        }
    }

}
