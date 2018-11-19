package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.data.Constant;
import io.joshworks.eventry.projections.Checkpointer;
import io.joshworks.eventry.projections.EventStreamHandler;
import io.joshworks.eventry.projections.Jsr223Handler;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.Projections;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Status;
import io.joshworks.eventry.projections.result.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ProjectionTask implements Callable<ExecutionResult> {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionTask.class);

    private final Projection projection;
    private final AtomicBoolean stopRequested = new AtomicBoolean();

    private final List<TaskItem> tasks = new ArrayList<>();

    private ProjectionTask(Projection projection, List<TaskItem> taskItems) {
        this.projection = projection;
        this.tasks.addAll(taskItems);
    }

    public static ProjectionTask create(IEventStore store, Projection projection, Checkpointer checkpointer) {
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

    @Override
    public ExecutionResult call() {
        List<TaskStatus> statuses = new ArrayList<>();
        for (TaskItem taskItem : tasks) {
            statuses.add(taskItem.stats());
            TaskStatus result = runTask(taskItem);
            if (Status.FAILED.equals(result.status)) {
                abort();
                break;
            }
        }
        return new ExecutionResult(projection.name, statuses);
    }

    private void abort() {
        logger.info("Stopping all tasks");
        tasks.forEach(TaskItem::stop);
    }

    ExecutionResult status() {
        return new ExecutionResult(projection.name, tasks.stream().map(TaskItem::stats).collect(Collectors.toList()));
    }

    State state() {
        State state = new State();
        for (TaskItem task : tasks) {
            state = task.aggregateState(state);
        }
        return state;
    }

    private TaskStatus runTask(TaskItem taskItem) {
        TaskStatus status = taskItem.call();
        if (stopRequested.get() || !Status.RUNNING.equals(taskItem.status())) {
            taskItem.stop();
        }
        return status;
    }

    //FIXME
    private static List<TaskItem> createTaskItems(IEventStore store, Projection projection, Checkpointer checkpointer) {
        List<TaskItem> tasks = new ArrayList<>();
//        Set<String> streams = projection.sources;
//
//        validateStreamNames(streams);
//
//        //single source or not parallel
//        if (isSingleSource(projection)) {
//            final EventSource source = createSequentialSource(store, projection);
//            ProjectionContext context = new ProjectionContext(store);
//            EventStreamHandler handler = createHandler(projection, context);
//            TaskItem taskItem = new TaskItem(source, checkpointer, handler, context, projection);
//            tasks.add(taskItem);
//        } else { //multi source and parallel
//            List<EventSource> sources = createParallelSource(store, projection);
//            for (EventSource source : sources) {
//                ProjectionContext context = new ProjectionContext(store);
//                EventStreamHandler handler = createHandler(projection, context);
//                TaskItem taskItem = new TaskItem(source, checkpointer, handler, context, projection);
//                tasks.add(taskItem);
//            }
//
//        }
        return tasks;
    }

    //FIXME
    private static List<EventSource> createParallelSource(IEventStore store, Projection projection) {
        List<EventSource> result = new ArrayList<>();
//        for (String stream : projection.sources) {
//            Set<String> singleStreamSet = Set.of(stream);
//            if (Projection.Type.CONTINUOUS.equals(projection.type)) {
//                LogPoller<EventRecord> poller = Constant.ALL_STREAMS.equals(stream) ? store.logPoller(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE) : store.streamPoller(singleStreamSet);
//                result.add(new ContinuousEventSource(poller, singleStreamSet));
//            } else {
//                //TODO add LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE to fromAll
//                LogIterator<EventRecord> iterator = store.fromStream(stream);
//                result.add(new OneTimeEventSource(iterator, singleStreamSet));
//            }
//        }
        return result;
    }

    //FIXME
    private static EventSource createSequentialSource(IEventStore store, Projection projection, Checkpointer checkpointer) {
//        Set<String> streams = projection.sources;
//        boolean isAllStream = isAllStream(projection);
//        if (Projection.Type.CONTINUOUS.equals(projection.type)) {
//            LogPoller<EventRecord> poller = isAllStream ? store.logPoller(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE) : store.streamPoller(streams);
//            return new ContinuousEventSource(poller, streams);
//        } else {
//            //TODO add LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE to fromAll
//            LogIterator<EventRecord> iterator = isAllStream ? store.fromAll() : store.zipStreams(streams);
//            return new OneTimeEventSource(iterator, streams);
//        }
        return null;
    }

    //TODO
//    private static Map<String, Integer> tracker(Projection projection, Checkpointer checkpointer) {
//        Map<String, Integer> tracker = new HashMap<>();
//
//        Set<String> keys = projectionTaskIds(projection);
//
//        for (String key :keys) {
//            Checkpointer.Checkpoint checkpoint = checkpointer.get(key);
//            tracker.put(source, NO_VERSION);
//            if(checkpoint != null) {
//                tracker.putAll(checkpoint.tracker);
//            }
//        }
//
//        projection.sources.stream()
//                .map(s -> projection.name + "-" + s)
//                .map(id -> Optional.ofNullable(checkpointer.get(id)))
//                .
//    }

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

    //FIXME
    public Map<String, TaskStatus> metrics() {
//        return tasks.stream().collect(Collectors.toMap(t -> t.uuid, TaskItem::stats));
        return null;
    }

    private static EventStreamHandler createHandler(Projection projection, ProjectionContext context) {
        return new Jsr223Handler(context, projection.script, Projections.ENGINE_NAME);
    }

    private static Set<String> projectionTaskIds(Projection projection) {
        if(!projection.parallel) {
            return Set.of(projection.name);
        }
        return projection.sources.stream().map(source -> projection.name + "-" + source).collect(Collectors.toSet());
    }

    public void stop() {
        stopRequested.set(true);
    }


}
