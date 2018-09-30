package io.joshworks.eventry.projections.meta;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.data.ProjectionCompleted;
import io.joshworks.eventry.data.ProjectionFailed;
import io.joshworks.eventry.data.ProjectionStarted;
import io.joshworks.eventry.data.ProjectionStopped;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class ProjectionManager {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionManager.class);

    private final ExecutorService executor = Executors.newFixedThreadPool(10);
    private final ProjectionHandlers handlers = new ProjectionHandlers();
    private final Consumer<EventRecord> systemRecordAppender;
    private final Map<String, StreamTask> running = new HashMap<>();

    public ProjectionManager(Consumer<EventRecord> systemRecordAppender) {
        this.systemRecordAppender = systemRecordAppender;
    }

    public void run(Projection projection, IEventStore store) {
        logger.info("Started projection '{}'", projection.name);

        Set<String> streams = projection.streams;

        //TODO add multiple streams

        LogIterator<EventRecord> streamRecords = store.fromStreamIter(streams.iterator().next());
        StreamTask streamTask = new StreamTask(streamRecords, store, projection);

        try {
            EventRecord eventRecord = ProjectionStarted.create(projection.name);
            systemRecordAppender.accept(eventRecord);

            running.put(projection.name, streamTask);

            CompletableFuture.supplyAsync(streamTask::execute, executor)
                    .thenAccept(this::processResult);


        } catch (Exception e) {
            logger.error("Script execution failed for projection " + projection.name, e);
        }
    }


    //FIXME this will call EventStore and no QueuedEventStore, which breaks the serial insertion
    private void processResult(ExecutionResult result) {
        System.out.println("RESULT: " + result);

        Projection projection = result.projection;
        ExecutionResult.Failure failure = result.failure;
        Metrics metrics = result.metrics;

        if(ExecutionResult.Status.COMPLETED.equals(result.type)) {
            EventRecord projectionCompleted = ProjectionCompleted.create(projection.name, metrics.processed);
            systemRecordAppender.accept(projectionCompleted);
        } else if(ExecutionResult.Status.STOPPED.equals(result.type)) {
            EventRecord projectionFailed = ProjectionStopped.create(projection.name, "STOPPED BY USER", metrics.processed, metrics.logPosition);
            systemRecordAppender.accept(projectionFailed);
        } else {
            EventRecord projectionCompleted = ProjectionFailed.create(projection.name, failure.reason, metrics.processed, failure.stream, failure.version);
            systemRecordAppender.accept(projectionCompleted);
        }
    }

    public Metrics status(String projectionName) {
        StreamTask task = running.get(projectionName);
        if(task != null) {
            return task.metrics();
        }
        return null;
    }

    public Set<String> running() {
        return new HashSet<>(running.keySet());
    }

    public void stop(String projectionName) {
        StreamTask task = running.get(projectionName);
        if(task != null) {
            logger.info("Stop request for {}",  projectionName);
            task.stop();
        }
    }

    public void stopAll() {
        for (String projection : running()) {
            stop(projection);
        }
    }

}
