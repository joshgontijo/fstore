package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.TaskResult;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProjectionTask {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionTask.class);

    private final Projection projection;
    private final ExecutorService executor;
    private final Map<String, Metrics> tasksMetrics = new HashMap<>();
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final ProjectionContext context;
    private final IEventStore store;

    public ProjectionTask(IEventStore store, Projection projection, ExecutorService executor) {
        this.store = store;
        this.context = new ProjectionContext(store);
        this.projection = projection;
        this.executor = executor;
    }

    public ExecutionResult execute() {

        ExecutionResult result = new ExecutionResult(projection.name, context.options());
        try {
            EventStreamHandler handler = createHandler(projection, context);
            StreamSource source = handler.source();
            validateSource(source);

            if (source.isSingleSource()) {
                TaskResult taskResult = runSequentially(handler, source.streams);
                result.addTask(taskResult);
            } else {
                List<TaskResult> taskResults = runInParallel(handler, source.streams);
                result.addTasks(taskResults);
            }

            return result;

        } catch (Exception e) {
            logger.error("Projection " + projection.name + " failed", e);
            TaskResult failed = TaskResult.failed(projection.name, context.state(), null, e, null, -1);
            return result.addTask(failed);
        }


    }

    private List<TaskResult> runInParallel(EventStreamHandler handler, Set<String> streams) {
        return streams.stream()
                .map(stream -> executor.submit(() -> runSequentially(handler, Set.of(stream))))
                .map(this::waitForCompletion)
                .collect(Collectors.toList());
    }

    private TaskResult waitForCompletion(Future<TaskResult> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TaskResult runSequentially(EventStreamHandler handler, Set<String> streams) {
        LogIterator<EventRecord> stream = store.zipStreamsIter(streams);
        try {
            String id = UUID.randomUUID().toString().substring(0, 8);
            tasksMetrics.put(id, new Metrics(streams));

            return run(id, handler, stream);
        } catch (Exception e) {
            //should never happen since 'run' is catching all exceptions
            logger.error("Failed running", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    private TaskResult run(String runId, EventStreamHandler handler, LogIterator<EventRecord> stream) {
        EventRecord record = null;
        Metrics metrics = this.tasksMetrics.get(runId);
        try {
            while (stream.hasNext()) {
                if (stopRequested.get()) {
                    return TaskResult.stopped(projection.name, context.state(), metrics);
                }
                record = stream.next();
                JsonEvent event = JsonEvent.from(record);
                if (handler.filter(event, context.state())) {
                    handler.onEvent(event, context.state());
                } else {
                    metrics.skipped++;
                }
                metrics.processed++;
                metrics.logPosition = stream.position();
            }

            System.out.println(context.state());

            return TaskResult.completed(projection.name, context.state(), metrics);

        } catch (Exception e) {
            logger.error("Projection " + projection.name + " failed", e);
            String currentStream = record != null ? record.stream : "(none)";
            int currentVersion = record != null ? record.version : -1;
            return TaskResult.failed(projection.name, context.state(), metrics, e, currentStream, currentVersion);
        }
    }


    private void validateSource(StreamSource streamSource) {
        if (streamSource.streams == null || streamSource.streams.isEmpty()) {
            throw new RuntimeException("Source must be provided");
        }
    }

    public Map<String, Metrics> metrics() {
        Map<String, Metrics> copy = new HashMap<>();
        for (Map.Entry<String, Metrics> entry : tasksMetrics.entrySet()) {
            copy.put(entry.getKey(), entry.getValue().copy());
        }
        return copy;
    }

    private EventStreamHandler createHandler(Projection projection, ProjectionContext context) {
        return new Jsr223Handler(context, projection.script, "nashorn");
//        return new ByType(context);
    }

    public void stop() {
        stopRequested.set(true);
    }

}
