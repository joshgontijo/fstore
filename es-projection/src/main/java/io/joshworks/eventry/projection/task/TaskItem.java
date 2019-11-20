package io.joshworks.fstore.projection.task;

import io.joshworks.fstore.projection.ScriptExecutionException;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.projection.Checkpointer;
import io.joshworks.fstore.projection.EventStreamHandler;
import io.joshworks.fstore.projection.Projection;
import io.joshworks.fstore.projection.State;
import io.joshworks.fstore.projection.result.Metrics;
import io.joshworks.fstore.projection.result.ScriptExecutionResult;
import io.joshworks.fstore.projection.result.Status;
import io.joshworks.fstore.projection.result.TaskError;
import io.joshworks.fstore.projection.result.TaskStatus;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

class TaskItem implements Callable<TaskStatus>, Closeable {

    private final Logger logger;

    private final String id;
    private final EventStreamHandler handler;
    private final ProjectionContext context;
    private final Projection projection;
    private final Checkpointer checkpointer;
    private final Metrics metrics = new Metrics();
    private final StreamTracker tracker = new StreamTracker();
    private final LogIterator<EventRecord> source;

    private volatile Status status = Status.NOT_STARTED;
    private volatile TaskError error;

    TaskItem(String id, LogIterator<EventRecord> source, Checkpointer checkpointer, EventStreamHandler handler, ProjectionContext context, Projection projection) {
        this.source = source;
        this.checkpointer = checkpointer;
        this.handler = handler;
        this.context = context;
        this.projection = projection;
        this.id = id;
        this.logger = Logging.namedLogger("projection-task", id);
        ScriptExecutionResult result = this.handler.onStart(context.state());
        publishEvents(result);
    }

    @Override
    public synchronized TaskStatus call() {
        if (Status.STOPPED.equals(status) || Status.FAILED.equals(status) || Status.COMPLETED.equals(status)) {
            return new TaskStatus(status, error, metrics);
        }
        try {
            //read
            List<EventRecord> batchRecords = bulkRead();
            if (!batchRecords.isEmpty()) {
                status = Status.RUNNING;
                //process
                ScriptExecutionResult result = process(batchRecords);
                //publish
                publishEvents(result);

                checkpoint(batchRecords);
                updateStatus(batchRecords, result);

                if (!source.hasNext()) {
                    status = noNextRecordStatus();
                }
            } else {
                status = noNextRecordStatus();
            }

        } catch (ScriptExecutionException e) {
            status = Status.FAILED;
            logger.error("Task item " + id + " failed", e);
            error = new TaskError(e.getMessage(), metrics.logPosition, e.event);
        } catch (Exception e) {
            status = Status.FAILED;
            logger.error("Internal error on processing event for task items " + id, e);
            error = new TaskError(e.getMessage(), metrics.logPosition, null);
        }

        return new TaskStatus(status, error, metrics);
    }

    private Status noNextRecordStatus() {
        return Projection.Type.CONTINUOUS.equals(projection.type) ? Status.AWAITING : Status.COMPLETED;
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
            metrics.lastEvent = EventId.toString(last.stream, last.version);
        }
        metrics.logPosition = source.position();
    }

    private void checkpoint(List<EventRecord> batchRecords) {
        batchRecords.forEach(tracker::update);
        checkpointer.checkpoint(id, context.state(), tracker.get());
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

    String id() {
        return id;
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(source);
    }

    synchronized void stop(StopReason reason) {
        ScriptExecutionResult result = handler.onStop(reason, context.state());
        publishEvents(result);
        if (!Status.FAILED.equals(status)) {
            status = Status.STOPPED;
        }
        close();
    }

    void deleteCheckpoint() {
        checkpointer.remove(this.id);
    }
}
