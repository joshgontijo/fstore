package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.Checkpointer;
import io.joshworks.eventry.projections.EventStreamHandler;
import io.joshworks.eventry.projections.Jsr223Handler;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.Projections;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.projections.result.ExecutionResult;
import io.joshworks.eventry.projections.result.Status;
import io.joshworks.eventry.projections.result.TaskError;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
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
            TaskStatus result = runTask(taskItem);
            statuses.add(result);
            if (Status.FAILED.equals(result.status)) {
                abort();
                break;
            }

        }
        return new ExecutionResult(projection.name, statuses);

    }

    private void abort() {
        logger.info("Stopping all tasks");
        tasks.forEach(ti -> ti.stop(StopReason.ABORTED));
    }

    //stops and clean checkpoint state
    public void complete() {
        logger.info("Completing all tasks");
        tasks.forEach(taskItem -> {
            taskItem.stop(StopReason.COMPLETED);
            taskItem.deleteCheckpoint();
        });
    }

    ExecutionResult status() {
        return new ExecutionResult(projection.name, tasks.stream().map(TaskItem::stats).collect(Collectors.toList()));
    }

    public State state() {
        State state = new State();
        for (TaskItem task : tasks) {
            state = task.aggregateState(state);
        }
        return state;
    }

    private TaskStatus runTask(TaskItem taskItem) {
        try {
            TaskStatus status = taskItem.call();
            if (stopRequested.get() && !Status.FAILED.equals(status.status)) {
                taskItem.stop(StopReason.ABORTED);
            }
            return status;
        } catch (Exception e) {
            logger.error("Error while running TaskItem" + taskItem.id(), e);
            return new TaskStatus(Status.FAILED, new TaskError("INTERNAL ERROR: " + e.getMessage(), -1, null), null);
        }
    }

    private static List<TaskItem> createTaskItems(IEventStore store, Projection projection, Checkpointer checkpointer) {
        List<TaskItem> tasks = new ArrayList<>();
        Set<String> streams = projection.sources;

        validateStreamNames(streams);

        boolean isAllStream = isAllStream(projection.sources);
        //single source or not parallel
        if (isSingleSource(projection)) {
            String taskId = projection.name;

            Checkpointer.Checkpoint checkpoint = checkpointer.get(taskId);

            //TODO refactor this
            LogIterator<EventRecord> source;
            if (checkpoint != null) {
                if (isAllStream) {//there will be only single one
                    StreamName lastProcessed = checkpoint.lastProcessed.iterator().next();
                    StreamName start = StreamName.of(lastProcessed.name(), lastProcessed.version() + 1);
                    source = store.fromAll(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE, start);
                } else {
                    Set<StreamName> mergedStreamStartVersions = mergeCheckpoint(projection, checkpoint);
                    source = store.fromStreams(mergedStreamStartVersions);
                }
            } else {
                if (isAllStream) {
                    source = store.fromAll(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE);
                } else {
                    Set<StreamName> streamNames = projection.sources.stream().map(StreamName::of).collect(Collectors.toSet());
                    source = store.fromStreams(streamNames);
                }
            }

            ProjectionContext context = new ProjectionContext(store);
            EventStreamHandler handler = createHandler(projection, context);
            TaskItem taskItem = new TaskItem(taskId, source, checkpointer, handler, context, projection);
            tasks.add(taskItem);
        } else { //multi source and parallel
            for (String stream : projection.sources) {
                String taskId = projection.name + "-" + stream;
                StreamName streamName = getSingleCheckpointOrCreate(checkpointer, stream, taskId);

                LogIterator<EventRecord> source = isAllStream ? store.fromAll(LinkToPolicy.IGNORE, SystemEventPolicy.IGNORE, streamName) : store.fromStream(streamName);
                ProjectionContext context = new ProjectionContext(store);
                EventStreamHandler handler = createHandler(projection, context);
                TaskItem taskItem = new TaskItem(taskId, source, checkpointer, handler, context, projection);
                tasks.add(taskItem);
            }

        }
        return tasks;
    }

    private static Set<StreamName> mergeCheckpoint(Projection projection, Checkpointer.Checkpoint checkpoint) {
        //All the streams
        Map<String, StreamName> allStreams = projection.sources.stream()
                .map(StreamName::of)
                .collect(Collectors.toMap(StreamName::name, Function.identity()));

        //from checkpoint
        Map<String, StreamName> checkpointStreams = checkpoint.lastProcessed.stream()
                .map(sn -> StreamName.of(sn.name(), sn.version() + 1)) //next version
                .collect(Collectors.toMap(StreamName::name, Function.identity()));

        //merge them together
        allStreams.putAll(checkpointStreams);
        return new HashSet<>(allStreams.values());
    }


    private static StreamName getSingleCheckpointOrCreate(Checkpointer checkpointer, String stream, String taskId) {
        Checkpointer.Checkpoint checkpoint = checkpointer.get(taskId);
        //Only single stream here
        if (checkpoint == null || checkpoint.lastProcessed.size() != 1) {
            return StreamName.of(stream);
        }
        return checkpoint.lastProcessed.iterator().next();
    }

    private static boolean isAllStream(Set<String> streams) {
        return streams.size() == 1 && streams.iterator().next().equals(SystemStreams.ALL);
    }

    private static void validateStreamNames(Set<String> streams) {
        if (!isAllStream(streams) && streams.contains(SystemStreams.ALL)) {
            throw new IllegalArgumentException("Multiple streams cannot contain '" + SystemStreams.ALL + "' stream");
        }
    }

    public static boolean isSingleSource(Projection projection) {
        //zipped streams is single source
        return projection.sources.size() == 1 || !projection.parallel;
    }

    public Map<String, TaskStatus> metrics() {
        return tasks.stream().collect(Collectors.toMap(TaskItem::id, TaskItem::stats));
    }

    private static EventStreamHandler createHandler(Projection projection, ProjectionContext context) {
        return new Jsr223Handler(context, projection.script, Projections.ENGINE_NAME);
    }

    public void stop() {
        stopRequested.set(true);
    }


}
