package io.joshworks.eventry.projections.task;

import io.joshworks.eventry.ScriptExecutionException;
import io.joshworks.eventry.data.StreamFormat;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.EventStreamHandler;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.Checkpointer;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.ScriptExecutionResult;
import io.joshworks.eventry.projections.result.Status;
import io.joshworks.eventry.projections.result.TaskError;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Logging;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;

class TaskItem implements Callable<TaskStatus>, Closeable {

    private final Logger logger;

    private final String uuid = UUID.randomUUID().toString().substring(0, 8);
    private final EventSource source;
    private final EventStreamHandler handler;
    private final ProjectionContext context;
    private final Projection projection;
    private final Checkpointer checkpointer;
    private final Metrics metrics = new Metrics();
    private final String stateKey;

    private Status status = Status.NOT_STARTED;
    private TaskError error;

    TaskItem(EventSource source, Checkpointer checkpointer, EventStreamHandler handler, ProjectionContext context, Projection projection) {
        this.source = source;
        this.checkpointer = checkpointer;
        this.handler = handler;
        this.context = context;
        this.projection = projection;
        //TODO doesnt work for parallel streams, projection name is used by default
        this.stateKey = projection.name;
        this.logger = Logging.namedLogger("projection-task", stateKey);
    }

    @Override
    public TaskStatus call() {
        status = Status.RUNNING;
        try {

            //read
            List<EventRecord> batchRecords = bulkRead();
            if (!batchRecords.isEmpty()) {
                //process
                ScriptExecutionResult result = process(batchRecords);
                //publish
                publishEvents(result);

                checkpoint();

                updateStatus(batchRecords, result);
            }

        } catch (ScriptExecutionException e) {
            status = Status.FAILED;
            logger.error("Task item " + uuid + " failed", e);
            error = new TaskError(e.getMessage(), metrics.logPosition, e.event);
        } catch (Exception e) {
            status = Status.FAILED;
            logger.error("Internal error on processing event", e);
            error = new TaskError(e.getMessage(), metrics.logPosition, null);
        }

        return new TaskStatus(status, error, metrics);
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

    TaskStatus stats() {
        return new TaskStatus(status, error, metrics);
    }

    Status status() {
        return status;
    }

    State aggregateState(State other) {
        return handler.aggregateState(other, this.state());
    }

    private State state() {
        return context.state();
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(source);
    }

    void stop() {
        if (!Status.FAILED.equals(status)) {
            status = Status.STOPPED;
        }
        close();
    }
}
