package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.data.ProjectionCompleted;
import io.joshworks.eventry.data.ProjectionFailed;
import io.joshworks.eventry.data.ProjectionStarted;
import io.joshworks.eventry.data.ProjectionStopped;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Failure;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.Status;
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
    private final Map<String, ProjectionTask> running = new HashMap<>();

    public ProjectionManager(Consumer<EventRecord> systemRecordAppender) {
        this.systemRecordAppender = systemRecordAppender;
    }

    public void run(Projection projection, IEventStore store) {
        logger.info("Started projection '{}'", projection.name);


        //TODO add multiple streams
        ProjectionTask projectionTask = new ProjectionTask(store, projection, executor);

        try {
            EventRecord eventRecord = ProjectionStarted.create(projection.name);
            systemRecordAppender.accept(eventRecord);

            running.put(projection.name, projectionTask);

            CompletableFuture.supplyAsync(projectionTask::execute, executor)
                    .thenAccept(this::processResult);


        } catch (Exception e) {
            logger.error("Script execution failed for projection " + projection.name, e);
        }
    }


    //FIXME this will call EventStore and no QueuedEventStore, which breaks the serial insertion
    private void processResult(ExecutionResult result) {
        System.out.println("RESULT: " + result);

        String projectionName = result.projectionName;
        Failure failure = result.failure;
        Metrics metrics = result.metrics;

        if(Status.COMPLETED.equals(result.status)) {
            EventRecord projectionCompleted = ProjectionCompleted.create(projectionName, metrics.processed);
            systemRecordAppender.accept(projectionCompleted);
        } else if(Status.STOPPED.equals(result.status)) {
            EventRecord projectionFailed = ProjectionStopped.create(projectionName, "STOPPED BY USER", metrics.processed, metrics.logPosition);
            systemRecordAppender.accept(projectionFailed);
        } else {
            EventRecord projectionCompleted = ProjectionFailed.create(projectionName, failure.reason, metrics.processed, failure.stream, failure.version);
            systemRecordAppender.accept(projectionCompleted);
        }
    }

    public Metrics status(String projectionName) {
        ProjectionTask task = running.get(projectionName);
        if(task != null) {
            return task.metrics();
        }
        return null;
    }

    public Set<String> running() {
        return new HashSet<>(running.keySet());
    }

    public void stop(String projectionName) {
        ProjectionTask task = running.get(projectionName);
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
