package io.joshworks.fstore.projection;

import io.joshworks.fstore.api.IEventStore;
import io.joshworks.fstore.data.ProjectionCompleted;
import io.joshworks.fstore.data.ProjectionFailed;
import io.joshworks.fstore.data.ProjectionStopped;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.projection.result.ExecutionResult;
import io.joshworks.fstore.projection.result.Status;
import io.joshworks.fstore.projection.result.TaskError;
import io.joshworks.fstore.projection.result.TaskStatus;
import io.joshworks.fstore.projection.task.ProjectionTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ProjectionExecutor implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionExecutor.class);

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10, new ProjectionThreadFactory());

    private final IEventStore eventStore;
    private final Map<String, ProjectionTask> running = new HashMap<>();
    private final Checkpointer checkpointer;

    public ProjectionExecutor(File root, IEventStore eventStore) {
        this.checkpointer = new Checkpointer(root);
        this.eventStore = eventStore;
    }

    void run(Projection projection, IEventStore store) {
        logger.info("Starting projection '{}'", projection.name);

        ProjectionTask projectionTask = ProjectionTask.create(store, projection, checkpointer);
        try {
            running.put(projection.name, projectionTask);
            runTask(projectionTask);

        } catch (Exception e) {
            logger.error("Script execution failed for projection " + projection.name, e);
        }
    }

    private void runTask(ProjectionTask projectionTask) {
        CompletableFuture.supplyAsync(projectionTask::call, executor)
                .exceptionally(t -> {
                    logger.error("Internal error while running projection", t);
                    return new ExecutionResult("UNKNOWN", Set.of(new TaskStatus(Status.FAILED, new TaskError(t.getMessage(), -1, null), null)));
                }).thenAccept(status -> processResult(status, projectionTask));
    }


    //FIXME this will call EventStore and no QueuedEventStore, which breaks the serial insertion
    private void processResult(ExecutionResult result, ProjectionTask projectionTask) {
        String projectionName = result.projectionName;
//        Failure failure = result.failure;
//        Metrics metrics = result.metrics;
        Status status = result.status;

        //schedule immediately, it has body
        if (Status.RUNNING.equals(status)) {
            this.runTask(projectionTask);
            return;
        }

        //awaiting for body, schedule in few seconds
        if (Status.AWAITING.equals(status)) {
            this.executor.schedule(() -> this.runTask(projectionTask), 5, TimeUnit.SECONDS);
            return;
        }

        long processed = result.tasks.stream().mapToLong(t -> t.metrics.processed).sum();
        if (Status.COMPLETED.equals(status)) {
            EventRecord completed = ProjectionCompleted.create(projectionName, processed);
            eventStore.append(completed);
            projectionTask.complete();
        } else if (Status.STOPPED.equals(status)) {
            EventRecord stopped = ProjectionStopped.create(projectionName, "STOPPED BY USER", processed);
            eventStore.append(stopped);
        } else if (Status.FAILED.equals(status)) {
            TaskError taskError = result.tasks.stream().filter(t -> t.taskError != null).map(t -> t.taskError).findFirst().get();
            EventRecord failed = ProjectionFailed.create(projectionName, taskError.reason, processed, taskError.stream, taskError.version);
            eventStore.append(failed);
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

    @Override
    public void close() {
        for (String projection : running()) {
            stop(projection);
        }
        executor.shutdown();
        checkpointer.close();
    }

    private static class ProjectionThreadFactory implements ThreadFactory {
        AtomicInteger counter = new AtomicInteger();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("projections-task-" + counter.getAndIncrement());
            return t;
        }
    }
}
