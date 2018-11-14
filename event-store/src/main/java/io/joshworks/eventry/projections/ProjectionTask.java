package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.ScriptExecutionException;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.data.StreamFormat;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;
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

    private ProjectionTask(IEventStore store, Projection projection, List<TaskItem> taskItems) {
        this.context = new ProjectionContext(store);
        this.projection = projection;
        this.tasks.addAll(taskItems);

    }

    public static ProjectionTask create(IEventStore store, Projection projection) {
        ProjectionContext context = new ProjectionContext(store);

        EventStreamHandler handler = createHandler(projection, context);
        SourceOptions source = handler.source();
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
        taskItem.run();
        if (stopRequested.get() || !Status.RUNNING.equals(taskItem.status)) {
            taskItem.stop();
        }
        return taskItem.status();
    }


    private static List<TaskItem> createTaskItems(IEventStore store, EventStreamHandler handler, SourceOptions sourceOptions, Projection projection) {

        List<TaskItem> tasks = new ArrayList<>();
        Projection.Type type = projection.type;
        Set<String> streams = sourceOptions.streams;

        validateStreamNames(streams);

        //single source or not parallel
        if (sourceOptions.isSingleSource()) {
            final EventSource source = createSequentialSource(store, type, sourceOptions);
            ProjectionContext context = new ProjectionContext(store);
            TaskItem taskItem = new TaskItem(source, handler, context, type, projection.batchSize);
            tasks.add(taskItem);
        } else { //multi source and parallel
            List<EventSource> sources = createParallelSource(store, type, sourceOptions);
            for (EventSource source : sources) {
                ProjectionContext context = new ProjectionContext(store);
                TaskItem taskItem = new TaskItem(source, handler, context, type, projection.batchSize);
                tasks.add(taskItem);
            }

        }
        return tasks;
    }

    private static List<EventSource> createParallelSource(IEventStore store, Projection.Type type, SourceOptions options) {
        List<EventSource> result = new ArrayList<>();
        for (String stream : options.streams) {
            Set<String> singleStreamSet = Set.of(stream);
            if (Projection.Type.CONTINUOUS.equals(type)) {
                LogPoller<EventRecord> poller = Constant.ALL_STREAMS.equals(stream) ? store.logPoller(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE) : store.streamPoller(singleStreamSet);
                result.add(new ContinuousEventSource(poller, singleStreamSet));
            } else {
                //TODO add LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE to fromAllIter
                LogIterator<EventRecord> iterator = store.fromStreamIter(stream);
                result.add(new OneTimeEventSource(iterator, singleStreamSet));
            }
        }
        return result;
    }

    private static EventSource createSequentialSource(IEventStore store, Projection.Type type, SourceOptions options) {
        Set<String> streams = options.streams;
        boolean isAllStream = options.isAllStream();
        if (Projection.Type.CONTINUOUS.equals(type)) {
            LogPoller<EventRecord> poller = isAllStream ? store.logPoller(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE) : store.streamPoller(streams);
            return new ContinuousEventSource(poller, streams);
        } else {
            //TODO add LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE to fromAllIter
            LogIterator<EventRecord> iterator = isAllStream ? store.fromAllIter() : store.zipStreamsIter(streams);
            return new OneTimeEventSource(iterator, streams);
        }
    }

    private static boolean isAll(Set<String> streams) {
        return streams.size() == 1 && streams.iterator().next().equals(Constant.ALL_STREAMS);
    }

    private static void validateStreamNames(Set<String> streams) {
        if (!isAll(streams) && streams.contains(Constant.ALL_STREAMS)) {
            throw new IllegalArgumentException("Multiple streams cannot contain '" + Constant.ALL_STREAMS + "' stream");
        }
    }

    private static void validateSource(SourceOptions sourceOptions) {
        if (sourceOptions.streams == null || sourceOptions.streams.isEmpty()) {
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
        private final Projection.Type type;
        private final int batchSize;
        private final Metrics metrics = new Metrics();


        private Status status = Status.NOT_STARTED;
        private TaskError error;

        private TaskItem(EventSource source, EventStreamHandler handler, ProjectionContext context, Projection.Type type, int batchSize) {
            this.source = source;
            this.handler = handler;
            this.context = context;
            this.type = type;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            status = Status.RUNNING;
            try {

                //read
                List<EventRecord> batchRecords = new ArrayList<>();
                int processed = 0;
                long readStart = System.currentTimeMillis();
                while (processed < batchSize && source.hasNext()) {
                    EventRecord record = source.next();
                    if (record.isLinkToEvent()) {
                        throw new IllegalStateException("Event source must resolve linkTo events");
                    }
                    batchRecords.add(record);
                    processed++;
                }
                metrics.readTime += (System.currentTimeMillis() - readStart);


                //process
                long processStart = System.currentTimeMillis();
                ScriptExecutionResult result = handler.processEvents(batchRecords, context.state());
                metrics.processTime += (System.currentTimeMillis() - processStart);


                //write
                context.handleScriptOutput(result.outputEvents);
                metrics.writeTime = (System.currentTimeMillis() - processStart);

                metrics.linkedEvents += result.linkToEvents;
                metrics.emittedEvents += result.emittedEvents;
                metrics.processed += batchRecords.size();

                if (!source.hasNext() && !Projection.Type.CONTINUOUS.equals(type)) {
                    IOUtils.closeQuietly(source);
                    status = Status.COMPLETED;
                }

                if (!batchRecords.isEmpty()) {
                    EventRecord last = batchRecords.get(batchRecords.size() - 1);
                    metrics.lastEvent = StreamFormat.toString(last.stream, last.version);
                }
                metrics.logPosition = source.logPosition();


            } catch (ScriptExecutionException e) {
                status = Status.FAILED;
                logger.error("Task item " + uuid + " failed", e);
                error = new TaskError(e.getMessage(), metrics.logPosition, e.event);
            } catch (Exception e) {
                status = Status.FAILED;
                logger.error("Internal error on processing event", e);
                error = new TaskError(e.getMessage(), metrics.logPosition, null);
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
            return !poller.headOfLog();
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
