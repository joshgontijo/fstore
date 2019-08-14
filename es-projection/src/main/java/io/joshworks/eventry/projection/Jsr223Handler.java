package io.joshworks.eventry.projection;

import io.joshworks.eventry.projection.result.ScriptExecutionResult;
import io.joshworks.eventry.projection.task.ProjectionContext;
import io.joshworks.eventry.projection.task.StopReason;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.utils.StringUtils;

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

    private static final String BASE_PROCESS_EVENTS_METHOD_NAME = "_process_events";

    private static final String ON_EVENT_METHOD_NAME = "onEvent";
    private static final String AGGREGATE_STATE_METHOD_NAME = "_aggregateState";
    private static final String ON_START_METHOD_NAME = "_onStart";
    private static final String ON_STOP_METHOD_NAME = "_onStop";
    private static final String CONFIG_METHOD_NAME = "config";
    private static final String INITIAL_STATE_METHOD_NAME = "state";

    private static final String CONFIG_STREAMS_FIELD_NAME = "streams";
    private static final String CONFIG_PARALLEL_FIELD_NAME = "parallel";
    private static final String CONFIG_BATCH_SIZE_FIELD_NAME = "batchSize";
    private static final String CONFIG_PUBLISH_STATE_FIELD_NAME = "publishState";
    private static final String TYPE_NAME_FIELD_NAME = "type";

    private static final String PROJECTION_NAME_FIELD_NAME = "name";

    private static final boolean DEFAULT_PARALLEL = false;
    private static final int DEFAULT_BATCH_SIZE = 10000;

    private final Invocable invocable;

    public Jsr223Handler(ProjectionContext ctx, String script, String engineName) {
        try {
            Consumer<Map<String, Object>> config = ctx::options;
            Consumer<Map<String, Object>> initialState = ctx::initialState;

            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
            engine.put(CONFIG_METHOD_NAME, config);
            engine.put(INITIAL_STATE_METHOD_NAME, initialState);

            InputStream is = this.getClass().getClassLoader().getResourceAsStream("projections/base.js");
            String baseScript = IOUtils.toString(is);
            engine.eval(baseScript);
            engine.eval(script);

            this.invocable = (Invocable) engine;

        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }


    @Override
    public ScriptExecutionResult onStart(State state) {
        try {
            Map<String, Object> result = (Map<String, Object>) invocable.invokeFunction(ON_START_METHOD_NAME, state);
            return toScriptExecutionResult(result);
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public ScriptExecutionResult onStop(StopReason reason, State state) {
        try {
            String reasonVal = reason == null ? "NONE" : reason.name();
            Map<String, Object> result = (Map<String, Object>) invocable.invokeFunction(ON_STOP_METHOD_NAME, reasonVal, state);
            return toScriptExecutionResult(result);
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public void onEvent(JsonEvent record, State state) {
        try {
            invocable.invokeFunction(ON_EVENT_METHOD_NAME, record, state);
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public State aggregateState(State first, State second) {
        try {
            Map<String, Object> stateMap = (Map<String, Object>) invocable.invokeFunction(AGGREGATE_STATE_METHOD_NAME, first, second);
            State state = new State();
            if (stateMap != null) {
                state.putAll(stateMap);
            }
            return second;
        } catch (Exception e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public ScriptExecutionResult processEvents(List<EventRecord> events, State state) throws ScriptExecutionException {
        Queue<JsonEvent> jsonEvents = null;
        try {
            jsonEvents = events.stream().map(JsonEvent::from).collect(Collectors.toCollection(ArrayDeque::new));
            Map<String, Object> result = (Map<String, Object>) invocable.invokeFunction(BASE_PROCESS_EVENTS_METHOD_NAME, jsonEvents, state);
            return toScriptExecutionResult(result);
        } catch (Exception e) {
            JsonEvent failed = jsonEvents != null && !jsonEvents.isEmpty() ? jsonEvents.poll() : null;
            throw new ScriptExecutionException(e, failed);
        }
    }

    private ScriptExecutionResult toScriptExecutionResult(Map<String, Object> result) {
        Collection<Object> values = result.values();
        List<Map<String, Object>> parsed = new ArrayList<>();
        for (Object value : values) {
            parsed.add((Map<String, Object>) value);
        }
        return new ScriptExecutionResult(parsed);
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

            Object sourceStreams = options.get(CONFIG_STREAMS_FIELD_NAME);
            Object projectionName = options.get(PROJECTION_NAME_FIELD_NAME);
            Object type = options.get(TYPE_NAME_FIELD_NAME);
            Object parallel = options.getOrDefault(CONFIG_PARALLEL_FIELD_NAME, DEFAULT_PARALLEL);
            Object batchSize = options.getOrDefault(CONFIG_BATCH_SIZE_FIELD_NAME, DEFAULT_BATCH_SIZE);

            Set<String> streams = getStreams(sourceStreams);
            Projection.Type theType = getType(type);
            validateProjectionName(projectionName);

            boolean isParallel = parallel != null && ((Boolean) parallel);
            int batchSizeVal = getBatchSize(batchSize);

            return new Projection(script, String.valueOf(projectionName), engineName, streams, theType, isParallel, batchSizeVal);

        } catch (Exception e) {
            throw new ScriptException("Script compilation error: " + e.getMessage(), e);
        }
    }


    private static void validateProjectionName(Object projectionName) {
        if (projectionName == null || StringUtils.isBlank(String.valueOf(projectionName))) {
            throw new ScriptException("No projection name provided");
        }
    }

    private static Set<String> getStreams(Object sourceStreams) {
        if (sourceStreams == null) {
            throw new ScriptException("No source stream provided");
        }
        Set<String> streams = new HashSet<>(((Map<String, String>) sourceStreams).values());
        streams.removeIf(String::isEmpty);
        if (streams.isEmpty()) {
            throw new ScriptException("No source stream provided");
        }
        return streams;
    }

    private static Projection.Type getType(Object type) {
        if (type == null || StringUtils.isBlank(String.valueOf(type))) {
            throw new ScriptException("Projection type must be provided");
        }
        try {
            return Projection.Type.valueOf(String.valueOf(type).trim().toUpperCase());
        } catch (Exception e) {
            throw new ScriptException("Invalid projection type '" + type + "'");
        }
    }

    private static int getBatchSize(Object batchSize) {
        try {
            return (int) batchSize;
        } catch (Exception e) {
            throw new ScriptException("Invalid batch size value: " + batchSize + ", value must a positive integer");
        }
    }

}
