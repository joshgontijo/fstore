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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProjectionTask {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionTask.class);

    private final Projection projection;
    private final AtomicBoolean stopRequested = new AtomicBoolean();

    private final List<TaskItem> tasks = new ArrayList<>();

    private ProjectionTask(Projection projection, List<TaskItem> taskItems) {
        this.projection = projection;
        this.tasks.addAll(taskItems);
    }

    public static ProjectionTask create(IEventStore store, Projection projection, ProjectionCheckpointer checkpointer) {
        List<TaskItem> taskItems = new ArrayList<>();
        try {
            taskItems = createTaskItems(store, projection, checkpointer);
            return new ProjectionTask(projection, taskItems);
        } catch (Exception e) {
            for (TaskItem taskItem : taskItems) {
                taskItem.close();
            }
            throw e;
        }
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
        return new ExecutionResult(projection.name, tasks.stream().map(TaskItem::status).collect(Collectors.toList()));
    }

   State state() {
        State state = new State();
        for (TaskItem task : tasks) {
            state = task.aggregateState(state);
        }
        return state;
    }

    private TaskStatus runTask(TaskItem taskItem) {
        taskItem.run();
        if (stopRequested.get() || !Status.RUNNING.equals(taskItem.status)) {
            taskItem.stop();
        }
        return taskItem.status();
    }


    private static List<TaskItem> createTaskItems(IEventStore store, Projection projection, ProjectionCheckpointer checkpointer) {
        List<TaskItem> tasks = new ArrayList<>();
        Set<String> streams = projection.sources;

        validateStreamNames(streams);

        //single source or not parallel
        if (isSingleSource(projection)) {
            final EventSource source = createSequentialSource(store, projection);
            ProjectionContext context = new ProjectionContext(store);
            EventStreamHandler handler = createHandler(projection, context);
            TaskItem taskItem = new TaskItem(source, checkpointer, handler, context, projection);
            tasks.add(taskItem);
        } else { //multi source and parallel
            List<EventSource> sources = createParallelSource(store, projection);
            for (EventSource source : sources) {
                ProjectionContext context = new ProjectionContext(store);
                EventStreamHandler handler = createHandler(projection, context);
                TaskItem taskItem = new TaskItem(source, checkpointer, handler, context, projection);
                tasks.add(taskItem);
            }

        }
        return tasks;
    }

    private static List<EventSource> createParallelSource(IEventStore store, Projection projection) {
        List<EventSource> result = new ArrayList<>();
        for (String stream : projection.sources) {
            Set<String> singleStreamSet = Set.of(stream);
            if (Projection.Type.CONTINUOUS.equals(projection.type)) {
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

    private static EventSource createSequentialSource(IEventStore store, Projection projection) {
        Set<String> streams = projection.sources;
        boolean isAllStream = isAllStream(projection);
        if (Projection.Type.CONTINUOUS.equals(projection.type)) {
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

    public static boolean isSingleSource(Projection projection) {
        //zipped streams is single source
        return projection.sources.size() == 1 || !projection.parallel;
    }

    public static boolean isAllStream(Projection projection) {
        return projection.sources.size() == 1 && Constant.ALL_STREAMS.equals(new LinkedList<>(projection.sources).getFirst());
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
        private final Projection projection;
        private final ProjectionCheckpointer checkpointer;
        private final Metrics metrics = new Metrics();
        private final String stateKey;

        private Status status = Status.NOT_STARTED;
        private TaskError error;

        private TaskItem(EventSource source, ProjectionCheckpointer checkpointer, EventStreamHandler handler, ProjectionContext context, Projection projection) {
            this.source = source;
            this.checkpointer = checkpointer;
            this.handler = handler;
            this.context = context;
            this.projection = projection;
            //TODO doesnt work for parallel streams, projection name is used by default
            this.stateKey = projection.name;
        }

        @Override
        public void run() {
            status = Status.RUNNING;
            try {

                //read
                List<EventRecord> batchRecords = bulkRead();
                //process
                ScriptExecutionResult result = process(batchRecords);
                //publish
                publishEvents(result);

                checkpoint();

                updateStatus(batchRecords, result);

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

        //TODO truncate stream up to last batch position if write fails
        private void publishEvents(ScriptExecutionResult result) {
            long publishStart = System.currentTimeMillis();
            context.publishEvents(result.outputEvents);
            metrics.publishTime = (System.currentTimeMillis() - publishStart);
        }

        private ScriptExecutionResult process(List<EventRecord> batchRecords) throws ScriptExecutionException {
            long processStart = System.currentTimeMillis();
            ScriptExecutionResult result = handler.processEvents(batchRecords, context.state());
            metrics.processTime += (System.currentTimeMillis() - processStart);
            return result;
        }

        private void updateStatus(List<EventRecord> batchRecords, ScriptExecutionResult result) {
            metrics.linkedEvents += result.linkToEvents;
            metrics.emittedEvents += result.emittedEvents;
            metrics.processed += batchRecords.size();

            if (!source.hasNext() && !Projection.Type.CONTINUOUS.equals(projection.type)) {
                IOUtils.closeQuietly(source);
                status = Status.COMPLETED;
            }

            if (!batchRecords.isEmpty()) {
                EventRecord last = batchRecords.get(batchRecords.size() - 1);
                metrics.lastEvent = StreamFormat.toString(last.stream, last.version);
            }
            metrics.logPosition = source.position();
        }

        private void checkpoint() {
            State state = context.state();
            Map<String, Integer> tracker = source.streamTracker();
            checkpointer.checkpoint(stateKey, state, tracker);
        }

        private List<EventRecord> bulkRead() {
            List<EventRecord> batchRecords = new ArrayList<>();
            int processed = 0;
            long readStart = System.currentTimeMillis();
            while (processed < projection.batchSize && source.hasNext()) {
                EventRecord record = source.next();
                if (record.isLinkToEvent()) {
                    throw new IllegalStateException("Event source must resolve linkTo events");
                }
                batchRecords.add(record);
                processed++;
            }
            metrics.readTime += (System.currentTimeMillis() - readStart);
            return batchRecords;
        }

        private TaskStatus status() {
            return new TaskStatus(status, error, context.state(), metrics);
        }

        private State aggregateState(State other) {
            return handler.aggregateState(other, this.state());
        }

        private State state() {
            return context.state();
        }

        @Override
        public void close() {
            IOUtils.closeQuietly(source);
        }

        private void stop() {
            status = Status.STOPPED;
            close();
        }
    }


    private interface EventSource extends Closeable {

        Set<String> streams();

        EventRecord next();

        boolean hasNext();

        long position();

        Map<String, Integer> streamTracker();
    }

    private static class OneTimeEventSource implements EventSource {

        private final LogIterator<EventRecord> iterator;
        private final Set<String> streams;
        private final StreamTracker tracker = new StreamTracker();


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
            return tracker.update(iterator.next());
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public long position() {
            return iterator.position();
        }

        @Override
        public Map<String, Integer> streamTracker() {
            return tracker.tracker();
        }

        @Override
        public void close() throws IOException {
            iterator.close();
        }
    }

    private static class ContinuousEventSource implements EventSource {

        private final LogPoller<EventRecord> poller;
        private final Set<String> streams;
        private final StreamTracker tracker = new StreamTracker();

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
                EventRecord record = poller.poll(5, TimeUnit.SECONDS);
                return tracker.update(record);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return !poller.headOfLog();
        }

        @Override
        public Map<String, Integer> streamTracker() {
            return tracker.tracker();
        }

        @Override
        public long position() {
            return poller.position();
        }

        @Override
        public void close() throws IOException {
            poller.close();
        }
    }

    private static class StreamTracker {

        private final Map<String, Integer> multiStreamTracker = new ConcurrentHashMap<>();

        private EventRecord update(EventRecord record) {
            if (record != null) {
                multiStreamTracker.put(record.stream, record.version);
            }
            return record;
        }

        private Map<String, Integer> tracker() {
            return new HashMap<>(multiStreamTracker);
        }

    }


}
