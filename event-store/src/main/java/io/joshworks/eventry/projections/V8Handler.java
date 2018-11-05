//package io.joshworks.eventry.projections;
//
//import com.eclipsesource.v8.Releasable;
//import com.eclipsesource.v8.V8;
//import com.eclipsesource.v8.V8Array;
//import com.eclipsesource.v8.V8Object;
//import com.eclipsesource.v8.utils.V8ObjectUtils;
//import io.joshworks.eventry.utils.StringUtils;
//
//import javax.script.ScriptEngine;
//import javax.script.ScriptEngineManager;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.function.BiConsumer;
//import java.util.function.Consumer;
//
//public class V8Handler implements EventStreamHandler {
//
//    private static final String ON_EVENT_METHOD_NAME = "onEvent";
//    private static final String FILTER_METHOD_NAME = "filter";
//    private static final String CONFIG_METHOD_NAME = "config";
//    private static final String INITIAL_STATE_METHOD_NAME = "state";
//
//    private static final String SOURCE_STREAMS_FIELD_NAME = "streams";
//    private static final String SOURCE_PARALLEL_FIELD_NAME = "parallel";
//    private static final String ENABLED_FIELD_NAME = "enabled";
//    private static final String PROJECTION_NAME_FIELD_NAME = "name";
//    private static final String TYPE_NAME_FIELD_NAME = "type";
//
//    private static final String EMIT_METHOD_NAME = "emit";
//    private static final String LINK_TO_METHOD_NAME = "linkTo";
//
//    private final V8 runtime = V8.createV8Runtime();
//    private final StreamSource source;
//
//    public V8Handler(ProjectionContext ctx, String script) {
//        try {
//            BiConsumer<String, JsonEvent> emmit = ctx::emit;
//            BiConsumer<String, JsonEvent> linkTo = ctx::linkTo;
//            Consumer<Map<String, Object>> config = ctx::options;
//            Consumer<Map<String, Object>> initialState = ctx::initialState;
//
//            runtime.registerJavaMethod((receiver, parameters) -> {
//                V8Object options = parameters.getObject(0);
//                Map<String, ? super Object> map = V8ObjectUtils.toMap(options);
//                config.accept(map);
//                release(receiver);
//            }, CONFIG_METHOD_NAME);
//
//            runtime.registerJavaMethod((receiver, parameters) -> {
//                V8Object state = parameters.getObject(0);
//                Map<String, ? super Object> map = V8ObjectUtils.toMap(state);
//                initialState.accept(map);
//                release(receiver);
//            }, INITIAL_STATE_METHOD_NAME);
//
//            runtime.registerJavaMethod((receiver, parameters) -> {
//                String stream = parameters.getString(0);
//                V8Object event = parameters.getObject(1);
//                Map<String, ? super Object> evMap = V8ObjectUtils.toMap(event);
//                emmit.accept(stream, JsonEvent.fromMap(evMap));
//                release(receiver);
//            }, EMIT_METHOD_NAME);
//
//            runtime.registerJavaMethod((receiver, parameters) -> {
//                String stream = parameters.getString(0);
//                V8Object event = parameters.getObject(1);
//                Map<String, ? super Object> evMap = V8ObjectUtils.toMap(event);
//                linkTo.accept(stream, JsonEvent.fromMap(evMap));
//                release(receiver);
//            }, LINK_TO_METHOD_NAME);
//
//
//            runtime.executeVoidScript(script);
////            runtime.release();
//
//            this.source = getStreamSource(ctx);
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    private static void release(V8Object receiver) {
//        if (receiver instanceof Releasable) {
//            ((Releasable) receiver).release();
//        }
//    }
//
//    private StreamSource getStreamSource(ProjectionContext ctx) {
//        try {
//            List<String> sourceStreams = (List<String>) ctx.options().get(SOURCE_STREAMS_FIELD_NAME);
//            Boolean parallel = (Boolean) ctx.options().get(SOURCE_PARALLEL_FIELD_NAME);
//            return new StreamSource(new HashSet<>(sourceStreams), parallel != null && parallel);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public boolean filter(JsonEvent record, State state) {
//        try {
//            V8Object eventObj = V8ObjectUtils.toV8Object(runtime, record.toMap());
//            V8Object stateObj = V8ObjectUtils.toV8Object(runtime, state);
//
//            V8Array parameters = new V8Array(runtime).push(eventObj).push(stateObj);
//            boolean result = runtime.executeBooleanFunction(FILTER_METHOD_NAME, parameters);
//            return result;
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public void onEvent(JsonEvent record, State state) {
//        try {
//            V8Object eventObj = V8ObjectUtils.toV8Object(runtime, record.toMap());
//            V8Object stateObj = V8ObjectUtils.toV8Object(runtime, state);
//
//            V8Array parameters = new V8Array(runtime).push(eventObj).push(stateObj);
//            runtime.executeVoidFunction(ON_EVENT_METHOD_NAME, parameters);
//
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public StreamSource source() {
//        return source;
//    }
//
//    static Projection compile(String script, String engineName) {
//        try {
//            Map<String, Object> options = new HashMap<>();
//            Map<String, Object> initialState = new HashMap<>();
//            ScriptEngine engine = new ScriptEngineManager().getEngineByName(engineName);
//            Consumer<Map<String, Object>> configConsumer = options::putAll;
//            Consumer<Map<String, Object>> initialStateConsumer = initialState::putAll;
//            engine.put(CONFIG_METHOD_NAME, configConsumer);
//            engine.put(INITIAL_STATE_METHOD_NAME, initialStateConsumer);
//
//            engine.eval(script);
//
//            Object sourceStreams = options.get(SOURCE_STREAMS_FIELD_NAME);
//            Object projectionName = options.get(PROJECTION_NAME_FIELD_NAME);
//            Object type = options.get(TYPE_NAME_FIELD_NAME);
//            Object parallel = options.get(SOURCE_PARALLEL_FIELD_NAME);
//            Object enabled = options.get(ENABLED_FIELD_NAME);
//
//            if (sourceStreams == null) {
//                throw new ScriptException("No source stream provided");
//            }
//            Set<String> streams = new HashSet<>(((Map<String, String>) sourceStreams).values());
//            streams.removeIf(String::isEmpty);
//            if (streams.isEmpty()) {
//                throw new ScriptException("No source stream provided");
//            }
//
//            if (projectionName == null || StringUtils.isBlank(String.valueOf(projectionName))) {
//                throw new ScriptException("No projection name provided");
//            }
//            if (type == null || StringUtils.isBlank(String.valueOf(type))) {
//                throw new ScriptException("No source stream provided");
//            }
//            Projection.Type theType = getType(type);
//
//            boolean isParallel = parallel != null && ((Boolean) parallel);
//            boolean isEnabled = enabled == null || ((Boolean) enabled);
//
//            return new Projection(script, String.valueOf(projectionName), engineName, streams, theType, isEnabled, isParallel);
//
//        } catch (Exception e) {
//            throw new CompilationException("Script compilation error: " + e.getMessage(), e);
//        }
//    }
//
//    private static Projection.Type getType(Object type) {
//        try {
//            return Projection.Type.valueOf(String.valueOf(type).trim().toUpperCase());
//        } catch (Exception e) {
//            throw new RuntimeException("Invalid projection type '" + type + "'");
//        }
//    }
//
//}
