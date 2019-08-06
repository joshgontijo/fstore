package io.joshworks.eventry.writer;

import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.fstore.core.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

public class EventWriter implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(EventWriter.class);
    private final WriteQueue[] writers;

    public EventWriter(IEventLog eventLog, Index index, int numWriters, int maxQueueSize) {
        this.writers = new WriteQueue[numWriters];
        for (int i = 0; i < numWriters; i++) {
            this.writers[i] = new WriteQueue(i, eventLog, index, maxQueueSize);
        }
    }

    public <R> Future<R> enqueue(long stream, Function<Writer, R> func) {
        return select(stream).submit(func);
    }

    public <R> Future<Void> enqueue(long stream, Consumer<Writer> func) {
        return select(stream).submit(func);
    }

    private WriteQueue select(long streamHash) {
        return writers[(int) (streamHash % writers.length)];
    }

    @Override
    public void close() {
        logger.info("Shutting down event writer");
        for (WriteQueue writer : writers) {
            writer.close();
        }

        for (WriteQueue writer : writers) {
            Threads.awaitTerminationOf(writer.executor, 2, TimeUnit.SECONDS, () -> logger.info("Waiting {} remaining writes to complete", writer.queue.size()));
        }
    }

    private static class WriteQueue implements Closeable {
        private final ThreadPoolExecutor executor;
        private final BlockingQueue<Runnable> queue;
        private final Writer writer;

        private WriteQueue(int id, IEventLog eventLog, Index index, int maxQueueSize) {
            this.queue = maxQueueSize < 0 ? new LinkedBlockingDeque<>() : new ArrayBlockingQueue<>(maxQueueSize);
            this.executor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.DAYS, queue, Threads.namedThreadFactory("event-writer-" + id), new ThreadPoolExecutor.AbortPolicy());
            this.writer = new Writer(eventLog, index);
        }

        private <R> Future<R> submit(Function<Writer, R> func) {
            return executor.submit(() -> func.apply(writer));
        }

        private <R> Future<Void> submit(Consumer<Writer> func) {
            return executor.submit(() -> {
                func.accept(writer);
                return null;
            });
        }

        @Override
        public void close() {
            executor.shutdown();
        }
    }
}
