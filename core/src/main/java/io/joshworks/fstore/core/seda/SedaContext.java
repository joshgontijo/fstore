package io.joshworks.fstore.core.seda;

import io.joshworks.fstore.core.util.Logging;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class SedaContext implements Closeable {

    private final Logger logger;

    private final Map<String, Stage> stages = new ConcurrentHashMap<>();
    private final AtomicReference<ContextState> state = new AtomicReference<>(ContextState.RUNNING);
    private final String name;

    public SedaContext(String name) {
        this.name = name;
        logger = Logging.namedLogger(name, "seda");
    }

    @SuppressWarnings("unchecked")
    public <T> void addStage(String name, StageHandler<T> handler, Stage.Builder builder) {
        if (stages.containsKey(name)) {
            throw new IllegalArgumentException("Duplicated stage name '" + name + "'");
        }
        Stage<T> stage = builder.build(name, handler, this);
        stages.put(name, stage);
    }

    public Map<String, StageStats> stats() {
        return stages.values().stream().collect(Collectors.toMap(Stage::name, Stage::stats));
    }

    public <R> CompletableFuture<R> submit(String stageName, Object event) {
        ContextState contextState = state.get();
        if (!ContextState.RUNNING.equals(contextState)) {
            throw new IllegalStateException("Cannot accept new events on stage '" + stageName + "', context state " + contextState);
        }
        Objects.requireNonNull(event, "Event must be provided");
        CompletableFuture<R> future = new CompletableFuture<>();
        sendTo(stageName, event, future);
        return future;
    }

    public ContextState state() {
        return state.get();
    }


    void sendToNextStage(String stageName, String correlationId, Object event, CompletableFuture<Object> future) {
        ContextState contextState = state.get();
        if (ContextState.CLOSED.equals(contextState) || ContextState.CLOSING.equals(contextState)) {
            throw new IllegalStateException("Cannot accept new events, context state " + contextState);
        }
        Objects.requireNonNull(event, "Event must be provided");
        sendTo(stageName, event, future);
    }

    @SuppressWarnings("unchecked")
    private <T> void sendTo(String stageName, Object event, CompletableFuture<T> future) {
        Stage stage = stages.get(stageName);
        if (stage == null) {
            throw new IllegalArgumentException("No such stage: " + stageName);
        }
        stage.submit(event, future);
    }

    public Set<String> stages() {
        return new HashSet<>(stages.keySet());
    }

    @Override
    public synchronized void close() {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.CLOSING)) {
            return;
        }
        closeInternal();
    }

    public synchronized void close(long timeout, TimeUnit unit) {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.CLOSING)) {
            return;
        }
        logger.info("Closing SEDA context");
        for (Stage stage : stages.values()) {
            stage.close(timeout, unit);
        }
        state.set(ContextState.CLOSED);
    }

    private void closeInternal() {
        logger.info("Closing SEDA context");
        stages.values().forEach(Stage::close);
        state.set(ContextState.CLOSED);
    }

    //Close the context and process remaining items in the stages
    public void shutdown() {
        if (!state.compareAndSet(ContextState.RUNNING, ContextState.AWAITING_COMPLETION)) {
            return;
        }

        boolean completed;
        long lastLog = System.currentTimeMillis();
        try {
            do {
                completed = true;
                for (Stage stage : stages.values()) {
                    StageStats stats = stage.stats();
                    boolean stageCompleted = stats.activeCount == 0 && stats.queueSize == 0;
                    completed = completed && stageCompleted;
                }

                if (!completed) {
                    long now = System.currentTimeMillis();
                    if (now - lastLog > TimeUnit.SECONDS.toMillis(5)) {
                        logger.info("Waiting for stages to complete");
                        lastLog = now;
                    }
                    sleep();
                }
            } while (!completed);
            logger.info("All stages completed");

        } finally {
            closeInternal();
        }
    }

    private void sleep() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
