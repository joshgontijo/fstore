package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.data.ProjectionCompleted;
import io.joshworks.eventry.data.ProjectionFailed;
import io.joshworks.eventry.data.ProjectionStarted;
import io.joshworks.eventry.data.ProjectionStopped;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.Status;
import io.joshworks.eventry.projections.result.TaskError;
import io.joshworks.eventry.projections.result.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ProjectionExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionExecutor.class);

    private final ExecutorService executor = Executors.newFixedThreadPool(10, new ThreadFactory() {
        AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("projections-task-" + counter.getAndIncrement());
            return t;
        }
    });
    private final Consumer<EventRecord> systemRecordAppender;
    private final Map<String, ProjectionTask> running = new HashMap<>();

    public ProjectionExecutor(Consumer<EventRecord> systemRecordAppender) {
        this.systemRecordAppender = systemRecordAppender;
    }

    void run(Projection projection, IEventStore store) {
        logger.info("Starting projection '{}'", projection.name);

        ProjectionTask projectionTask = ProjectionTask.create(store, projection);

        try {
            EventRecord eventRecord = ProjectionStarted.create(projection.name);
            systemRecordAppender.accept(eventRecord);
            running.put(projection.name, projectionTask);
            scheduleRun(projectionTask);

        } catch (Exception e) {
            logger.error("Script execution failed for projection " + projection.name, e);
        }
    }

    private void scheduleRun(ProjectionTask projectionTask) {
        executor.execute(() -> {
            ExecutionResult result = projectionTask.run();
            processResult(result);
            if (Status.RUNNING.equals(result.status)) {
                this.scheduleRun(projectionTask);
            }
        });
    }


    //FIXME this will call EventStore and no QueuedEventStore, which breaks the serial insertion
    private void processResult(ExecutionResult result) {
        String projectionName = result.projectionName;
//        Failure failure = result.failure;
//        Metrics metrics = result.metrics;
        Status status = result.status;

        if (Status.RUNNING.equals(status)) {
            return;
        }

        long processed = result.tasks.stream().mapToLong(t -> t.metrics.processed).sum();
        if (Status.COMPLETED.equals(status)) {
            EventRecord completed = ProjectionCompleted.create(projectionName, processed);
            systemRecordAppender.accept(completed);
        } else if (Status.STOPPED.equals(status)) {
            Metrics metrics = result.tasks.stream().map(t -> t.metrics).findFirst().get();
            EventRecord stopped = ProjectionStopped.create(projectionName, "STOPPED BY USER", processed, metrics.logPosition);
            systemRecordAppender.accept(stopped);
        } else if (Status.FAILED.equals(status)) {
            TaskError taskError = result.tasks.stream().filter(t -> t.taskError != null).map(t -> t.taskError).findFirst().get();
            EventRecord failed = ProjectionFailed.create(projectionName, taskError.reason, processed, taskError.stream, taskError.version);
            systemRecordAppender.accept(failed);
        } else {
            throw new RuntimeException("Invalid task status " + status);
        }
    }

    public Map<String, TaskStatus> status(String projectionName) {
        ProjectionTask task = running.get(projectionName);
        if (task != null) {
            return task.metrics();
        }
        return new HashMap<>();
    }

    public Set<String> running() {
        return new HashSet<>(running.keySet());
    }

    public void stop(String projectionName) {
        ProjectionTask task = running.get(projectionName);
        if (task != null) {
            logger.info("Stop request for {}", projectionName);
            task.stop();
        }
    }

    public void stopAll() {
        for (String projection : running()) {
            stop(projection);
        }
    }

}
