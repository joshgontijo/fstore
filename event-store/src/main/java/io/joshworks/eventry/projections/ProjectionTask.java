package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.data.StreamFormat;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.Status;
import io.joshworks.eventry.projections.result.TaskError;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.LogPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProjectionTask {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionTask.class);

    private final Projection projection;
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final ProjectionContext context;

    private final List<TaskItem> tasks = new ArrayList<>();

    private static final int BATCH_SIZE = 10000;

    private ProjectionTask(IEventStore store, Projection projection, List<TaskItem> taskItems) {
        this.context = new ProjectionContext(store);
        this.projection = projection;
        this.tasks.addAll(taskItems);

    }

    public static ProjectionTask create(IEventStore store, Projection projection) {
        ProjectionContext context = new ProjectionContext(store);

        EventStreamHandler handler = createHandler(projection, context);
        StreamSource source = handler.source();
        validateSource(source);

        List<TaskItem> taskItems = createTaskItems(store, handler, source, projection);
        return new ProjectionTask(store, projection, taskItems);

    }

    ExecutionResult run() {
        for (TaskItem taskItem : tasks) {
            TaskStatus result = runTask(taskItem);
            if (Status.FAILED.equals(result.status)) {
                abort();
                break;
            }
        }
        return status();
    }

    private void abort() {
        logger.info("Stopping all tasks");
        for (TaskItem taskItem : tasks) {
            taskItem.stop();
            if (!Status.FAILED.equals(taskItem.status)) {
                taskItem.status = Status.STOPPED;
            }
        }
    }

    ExecutionResult status() {
        return new ExecutionResult(projection.name, context.options(), tasks.stream().map(TaskItem::status).collect(Collectors.toList()));
    }

    private TaskStatus runTask(TaskItem taskItem) {
        for (int i = 0; i < BATCH_SIZE; i++) {
            taskItem.run();
            if (stopRequested.get() || !Status.RUNNING.equals(taskItem.status)) {
                taskItem.stop();
                break;
            }
        }
        return taskItem.status();
    }


    private static List<TaskItem> createTaskItems(IEventStore store, EventStreamHandler handler, StreamSource streamSource, Projection projection) {

        List<TaskItem> tasks = new ArrayList<>();
        Projection.Type type = projection.type;
        Set<String> streams = streamSource.streams;

        if (streamSource.isSingleSource()) {
            final EventSource source = createSource(store, type, streams);
            ProjectionContext context = new ProjectionContext(store);
            TaskItem taskItem = new TaskItem(source, handler, context);
            tasks.add(taskItem);

        } else {
            for (String stream : streams) {
                final EventSource source = createSource(store, type, streams);
                ProjectionContext context = new ProjectionContext(store);
                TaskItem taskItem = new TaskItem(source, handler, context);
                tasks.add(taskItem);
            }
        }

        return tasks;
    }

    private static EventSource createSource(IEventStore store, Projection.Type type, Set<String> streams) {
        if (Projection.Type.CONTINUOUS.equals(type)) {
            LogPoller<EventRecord> poller = isAll(streams) ? store.logPoller() : store.streamPoller(streams);
            return new ContinuousEventSource(poller, streams);
        } else {
            LogIterator<EventRecord> iterator = isAll(streams) ? store.fromAllIter() : store.zipStreamsIter(streams);
            return new OneTimeEventSource(iterator, streams);
        }
    }

    private static boolean isAll(Set<String> streams) {
        return streams.size() == 1 && streams.iterator().next().equals(Constant.ALL_STREAMS);
    }

    private static void validateSource(StreamSource streamSource) {
        if (streamSource.streams == null || streamSource.streams.isEmpty()) {
            throw new RuntimeException("Source must be provided");
        }
    }

    public Map<String, TaskStatus> metrics() {
        return tasks.stream().collect(Collectors.toMap(t -> t.uuid, TaskItem::status));
    }

    private static EventStreamHandler createHandler(Projection projection, ProjectionContext context) {
        return new Jsr223Handler(context, projection.script, Projections.ENGINE_NAME);
    }

    public void stop() {
        stopRequested.set(true);
    }


    private static class TaskItem implements Runnable, Closeable {

        private final String uuid = UUID.randomUUID().toString().substring(0, 8);
        private final EventSource source;
        private final EventStreamHandler handler;
        private final ProjectionContext context;
        private final Metrics metrics = new Metrics();

        private Status status = Status.NOT_STARTED;
        private TaskError error;

        private TaskItem(EventSource source, EventStreamHandler handler, ProjectionContext context) {
            this.source = source;
            this.handler = handler;
            this.context = context;
        }

        @Override
        public void run() {
            status = Status.RUNNING;
            JsonEvent event = null;
            try {
                if (!source.hasNext()) {
                    source.close();
                    status = Status.COMPLETED;
                }
                EventRecord record = source.next();
                if (record.isLinkToEvent()) {
                    throw new IllegalStateException("Event source must resolve linkTo events");
                }
                event = JsonEvent.from(record);
                if (handler.filter(event, context.state())) {
                    handler.onEvent(event, context.state());
                } else {
                    metrics.skipped++;
                }
                metrics.processed++;
                metrics.logPosition = source.logPosition();
                metrics.lastEvent = StreamFormat.toString(event.stream, event.version);

            } catch (Exception e) {
                logger.error("Task item " + uuid + " failed", e);
                String currentStream = event != null ? event.stream : "(none)";
                int currentVersion = event != null ? event.version : -1;
                status = Status.AWAITING;
                error = new TaskError(e.getMessage(), metrics.logPosition, currentStream, currentVersion, event);
            }

        }

        private TaskStatus status() {
            return new TaskStatus(status, error, context.state(), metrics);
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(source);
        }

        public void stop() {
            status = Status.STOPPED;
            close();
        }
    }


    private interface EventSource extends Closeable {

        Set<String> streams();

        EventRecord next();

        boolean hasNext();

        long logPosition();
    }

    private static class OneTimeEventSource implements EventSource {

        private final LogIterator<EventRecord> iterator;
        private Set<String> streams;

        private OneTimeEventSource(LogIterator<EventRecord> iterator, Set<String> streams) {
            this.iterator = iterator;
            this.streams = streams;
        }

        @Override
        public Set<String> streams() {
            return streams;
        }

        @Override
        public EventRecord next() {
            return iterator.next();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public long logPosition() {
            return iterator.position();
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }

    private static class ContinuousEventSource implements EventSource {

        private final LogPoller<EventRecord> poller;
        private Set<String> streams;

        private ContinuousEventSource(LogPoller<EventRecord> poller, Set<String> streams) {
            this.poller = poller;
            this.streams = streams;
        }

        @Override
        public Set<String> streams() {
            return streams;
        }

        @Override
        public EventRecord next() {
            try {
                return poller.poll(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public long logPosition() {
            return poller.position();
        }

        @Override
        public void close() throws IOException {
            poller.close();
        }
    }


}
