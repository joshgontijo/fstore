package io.joshworks.eventry.projections;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.data.ProjectionCompleted;
import io.joshworks.eventry.data.ProjectionFailed;
import io.joshworks.eventry.data.ProjectionStarted;
import io.joshworks.eventry.data.ProjectionStopped;
import io.joshworks.eventry.log.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Consumer;

public class ProjectionTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProjectionTask.class);

    final Projection projection;
    private ScriptExecution scriptExecution;
    private Consumer<EventRecord> systemRecordAppender;
    private Map<String, ExecutionStatus> tracker;
    private final IEventStore store;

    public ProjectionTask(Projection projection, IEventStore store, Consumer<EventRecord> systemRecordAppender, Map<String, ExecutionStatus> tracker) {
        this.projection = projection;
        this.store = store;
        this.scriptExecution = new ScriptExecution(store, this::onExecutionStatusUpdate);
        this.systemRecordAppender = systemRecordAppender;
        this.tracker = tracker;
    }

    @Override
    public void run() {
        logger.info("Started projection '{}'", projection.name);
        //TODO execution status only work for SingleStream ??
        tracker.put(projection.name, new ExecutionStatus(ExecutionStatus.State.RUNNING, null, -1, 0, 0));

        try {
            EventRecord eventRecord = ProjectionStarted.create(projection.name);
            systemRecordAppender.accept(eventRecord);

            scriptExecution.execute(projection.script);

            ExecutionStatus executionStatus = tracker.get(projection.name);
            EventRecord projectionCompleted = ProjectionCompleted.create(projection.name, executionStatus.processedItems);

            systemRecordAppender.accept(projectionCompleted);

        } catch (StopRequest e) {
            logger.warn("Stop request " + projection.name, e);
            ExecutionStatus executionStatus = tracker.get(projection.name);
            EventRecord projectionStopped = ProjectionStopped.create(projection.name, e.getMessage(), executionStatus.processedItems);
            systemRecordAppender.accept(projectionStopped);
        } catch (Exception e) {
            logger.error("Script execution failed for projection " + projection.name, e);
            ExecutionStatus executionStatus = tracker.get(projection.name);
            EventRecord projectionFailed = ProjectionFailed.create(projection.name, e.getMessage(), executionStatus.processedItems, executionStatus.stream, executionStatus.version);
            systemRecordAppender.accept(projectionFailed);
        }
    }

    public void stop() {
        scriptExecution.stop();
    }

    private void onExecutionStatusUpdate(ExecutionStatus status) {
        tracker.put(projection.name, status);
    }
}
