package io.joshworks.eventry.projections;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ScriptStreamBase {

    protected final Consumer<ExecutionStatus> executionStatusListener;
    protected final Supplier<Boolean> stopRequest;
    protected final AtomicLong processedItems = new AtomicLong();


    public ScriptStreamBase(Consumer<ExecutionStatus> executionStatusListener, Supplier<Boolean> stopRequest) {
        this.executionStatusListener = executionStatusListener;
        this.stopRequest = stopRequest;
    }


    protected void handleEvent(ScriptObjectMirror handlers, JsonEvent event, Stream<JsonEvent> stream, Map<String, Object> state) {
        checkStopRequest(event, stream);
        processedItems.incrementAndGet();
        executionStatusListener.accept(new ExecutionStatus(ExecutionStatus.State.RUNNING, event.stream, event.version, processedItems.get()));

        if (handlers.containsKey(event.type)) {
            handlers.callMember(event.type, state, event);
        }
        if (handlers.containsKey("_any")) {
            handlers.callMember(event.type, state, event);
        }
    }

    protected void checkStopRequest(JsonEvent event, Stream<JsonEvent> stream) {
        if (stopRequest.get()) {
            stream.close();
            executionStatusListener.accept(new ExecutionStatus(ExecutionStatus.State.STOPPED, event.stream, event.version, processedItems.get()));
            throw new StopRequest();
        }
    }


}
