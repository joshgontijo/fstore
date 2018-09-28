package io.joshworks.eventry.projections;

import jdk.nashorn.api.scripting.ScriptObjectMirror;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class ForEachStream extends ScriptStreamBase {

    private final Map<String, Stream<JsonEvent>> streams;
    private Map<String, Object> original = new HashMap<>();
    private Map<String, Map<String, Object>> streamState = new HashMap<>();

    //TODO move to ProjectionWorker ?
    private final ExecutorService executor = Executors.newFixedThreadPool(5, new ThreadFactory() {
        final AtomicLong counter = new AtomicLong();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("projection-stream-task-" + counter.incrementAndGet());
            return t;
        }
    });


    public ForEachStream(
            Map<String, Stream<JsonEvent>> streams,
            Consumer<ExecutionStatus> executionStatusListener,
            Supplier<Boolean> shutdownRequest) {
        super(executionStatusListener, shutdownRequest);
        this.streams = streams;
    }

    public ForEachStream withState(Map<String, Object> state) {
        this.original = state;
        return this;
    }

    public ForEachStream when(ScriptObjectMirror handlers) {
        streams.forEach((key, value) -> {
            Map<String, Object> streamState = this.streamState.putIfAbsent(key, new HashMap<>(original));
            executor.submit(() -> {
                value.forEach(event -> {
                    handleEvent(handlers, event, value, streamState);
                });
            });
        });
        return this;
    }

    public ForEachStream persistState() {
        //TODO
        System.out.println("Persisting stream states");
        streamState.forEach((k,v) -> {
            System.out.println(Arrays.toString(v.entrySet().toArray()));
        });

        return this;
    }

}
