package io.joshworks.eventry.projections;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class SingleStream extends ScriptStreamBase {

    private final Stream<JsonEvent> stream;
    private Map<String, Object> state = new HashMap<>();


    public SingleStream(
            Stream<JsonEvent> stream,
            Consumer<ExecutionStatus> executionStatusListener,
            Supplier<Boolean> shutdownRequest) {
        super(executionStatusListener, shutdownRequest);
        this.stream = stream;
    }

    public SingleStream withState(Map<String, Object> state) {
        this.state = state;
        return this;
    }

    public SingleStream filter(Predicate<? super JsonEvent> filter) {
        return new SingleStream(stream.filter(filter), executionStatusListener, stopRequest);
    }

    public SingleStream forEach(BiConsumer<Map<String, Object>, ? super JsonEvent> handler) {
        stream.forEach(event -> {
            checkStopRequest(event, stream);
            processedItems.incrementAndGet();

            long start = System.currentTimeMillis();
            handler.accept(state, event);
            long end = System.currentTimeMillis();

            executionStatusListener.accept(new ExecutionStatus(ExecutionStatus.State.RUNNING, event.stream, event.version, processedItems.get(), processedTime.addAndGet((end - start))));
        });
        return this;
    }

    public SingleStream when(ScriptObjectMirror handlers) {
        stream.takeWhile(event -> !stopRequest.get())
                .forEach(event -> {
                    handleEvent(handlers, event, stream, state);
                });
        return this;
    }

    public SingleStream persistState() {
        //TODO
        System.out.println("Persisting state");
        System.out.println(Arrays.toString(state.entrySet().toArray()));
        return this;
    }

}


