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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProjectionTask {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionTask.class);

    private final Projection projection;
    private final ExecutorService executor;
    //TODO where should it be placed ? ProjectionContext ?
    private final Metrics metrics = new Metrics();
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

        EventStreamHandler handler = createHandler(projection, context);
        StreamSource source = handler.source();
        validateSource(source);

        List<TaskResult> tasks = new ArrayList<>();
        if (source.isSingleSource()) {
            TaskResult taskResult = runSequentially(handler, source.streams);
            tasks.add(taskResult);
        } else {
            List<TaskResult> taskResults = runInParallel(handler, source.streams);
            tasks.addAll(taskResults);
        }

        ExecutionResult result;
        result.setTasks(tasks);

        return result;
    }

    private List<TaskResult> runInParallel(EventStreamHandler handler, Set<String> streams) {

        streams.stream()
                .map(stream -> executor.submit(() -> runSequentially(handler, streams)))
                .map(this::waitForCompletion)


    }

    private ExecutionResult waitForCompletion(Future<ExecutionResult> future) {
        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private TaskResult runSequentially(EventStreamHandler handler, Set<String> streams) {
        LogIterator<EventRecord> stream = store.zipStreamsIter(streams);
        try {
            return run(handler, streams, stream);
        } catch (Exception e) {
            //should never happen since 'run' is catching all exceptions
            logger.error("Failed running", e);
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(stream);
        }
    }

    private TaskResult run(EventStreamHandler handler, Set<String> streams, LogIterator<EventRecord> stream) {
        EventRecord record = null;
        try {
            while (stream.hasNext()) {
                if (stopRequested.get()) {
                    return ExecutionResult.stopped(projection.name, streams, context, metrics);
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

            return ExecutionResult.completed(projection.name, streams, context, metrics);

        } catch (Exception e) {
            logger.error("Projection " + projection.name + " failed", e);
            String currentStream = record != null ? record.stream : "(none)";
            int currentVersion = record != null ? record.version : -1;
            return ExecutionResult.failed(projection.name, streams, context, metrics, e, currentStream, currentVersion);
        }
    }


    private void validateSource(StreamSource streamSource) {
        if (streamSource.streams == null || streamSource.streams.isEmpty()) {
            throw new RuntimeException("Source must be provided");
        }
    }

    public Metrics metrics() {
        return metrics.copy();
    }

    private EventStreamHandler createHandler(Projection projection, ProjectionContext context) {
//        return new Jsr223Handler(context, projection.script, "nashorn");
        return new ByType(context);
    }

    public void stop() {
        stopRequested.set(true);
    }

}
