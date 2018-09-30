package io.joshworks.eventry.projections.meta;

import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.JsonEvent;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

public class StreamTask  {

    private static final Logger logger = LoggerFactory.getLogger(StreamTask.class);

    private final LogIterator<EventRecord> stream;
    private final Projection projection;
    //TODO where should it be placed ? ProjectionContext ?
    private final Metrics metrics = new Metrics();
    private final AtomicBoolean stopRequested = new AtomicBoolean();
    private final ProjectionContext context;

    public StreamTask(LogIterator<EventRecord> dataStream, IEventStore store, Projection projection) {
        this.stream = dataStream;
        this.context =  new ProjectionContext(store);
        this.projection = projection;
    }

    public ExecutionResult execute() {

        EventRecord record = null;
        try {
            EventStreamHandler handler = createHandler(projection, context);

            while (stream.hasNext()) {
                if (stopRequested.get()) {
                    return ExecutionResult.stopped(projection, context, metrics);
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

            return ExecutionResult.completed(projection, context, metrics);
        } catch (Exception e) {
            logger.error("Projection " + projection.name + " failed", e);
            String currentStream = record != null ? record.stream : "(none)";
            int currentVersion = record != null ? record.version : -1;
            return ExecutionResult.failed(projection, context, metrics, e, currentStream, currentVersion);
        } finally {
            IOUtils.closeQuietly(stream);
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
