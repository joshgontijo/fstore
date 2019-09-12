package io.joshworks.eventry.writer;

import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class EventWriter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EventWriter.class);
    public static final String EVENT_WRITER = "event-writer";
    private final Writer writer;
    private final ExecutorService executor;
    private final BlockingQueue<Runnable> queue;

    public EventWriter(IEventLog eventLog, Index index, int maxQueueSize) {
        this.queue = maxQueueSize < 0 ? new LinkedBlockingDeque<>() : new ArrayBlockingQueue<>(maxQueueSize);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, queue, Threads.namedThreadFactory(EVENT_WRITER), new ThreadPoolExecutor.AbortPolicy());
        this.executor = new MonitoredThreadPool(EVENT_WRITER, executor);
        this.writer = new Writer(eventLog, index);

    }

    public <R> CompletableFuture<R> queue(Function<Writer, R> func) {
        return CompletableFuture.supplyAsync(() -> func.apply(writer), executor);
    }

    @Override
    public void close() {
        logger.info("Shutting down event writer");
        Threads.awaitTerminationOf(executor, 2, TimeUnit.SECONDS, () -> logger.info("Waiting {} remaining writes to complete", queue.size()));
    }
}
