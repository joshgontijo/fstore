package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ScriptExecution {

    private final ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
    private final ScriptAPI api;
    private IEventStore store;
    private AtomicBoolean stopRequested = new AtomicBoolean();

    private Map<String, Object> options = new HashMap<>();

    public ScriptExecution(IEventStore store, Consumer<ExecutionStatus> onExecutionStatusUpdate) {
        this.store = store;
        this.api = new ScriptAPI(store, onExecutionStatusUpdate, this::stopRequested);

        engine.put("options", (Consumer<Map<String, Object>>) this::options);

        engine.put("fromStream", api.fromStream);
        engine.put("fromStreams", api.fromStreams);
        engine.put("foreachStream", api.forEachStream);

        engine.put("emit", api.emit);
        engine.put("linkTo", api.linkTo);
    }

    private boolean stopRequested() {
        return stopRequested.get();
    }

    public void stop() {
        stopRequested.set(true);
    }

    void options(Map<String, Object> options) {
        this.options = options;
    }

    public void execute(String script) throws ScriptException {
        engine.eval(script);
    }

}
