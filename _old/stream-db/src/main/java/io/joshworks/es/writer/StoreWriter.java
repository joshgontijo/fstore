package io.joshworks.es.writer;

import io.joshworks.es.events.LinkToEvent;
import io.joshworks.es.events.SystemStreams;
import io.joshworks.es.events.WriteEvent;
import io.joshworks.es.index.Index;
import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import io.joshworks.es.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StoreWriter {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final BlockingQueue<WriteTask> tasks;
    private final long poolWait;
    private final BatchingWriter writer;
    private final List<WriteTask> pending = new ArrayList<>();
    private final Thread thread = new Thread(this::process, "writer");

    public StoreWriter(Log log, Index index, int writeQueueSize, int maxBatchSize, int bufferSize, long poolWait) {
        this.poolWait = poolWait;
        this.writer = new BatchingWriter(log, index, maxBatchSize, bufferSize);
        this.tasks = new ArrayBlockingQueue<>(writeQueueSize);
    }

    //No need for StoreLock here as it will always be appending to the head
    public WriteTask enqueue(WriteEvent event) {
        return enqueue(() -> writer.append(event));
    }

    public WriteTask enqueue(LinkToEvent event) {
        return enqueue(() -> {
            IndexEntry ie = writer.findEquals(IndexKey.of(event.srcStream(), event.srcVersion()));
            if (ie == null) {
                throw new IllegalArgumentException("No such event " + IndexKey.toString(event.srcStream(), event.srcVersion()));
            }
            WriteEvent linkTo = SystemStreams.linkTo(event);
            writer.append(linkTo);
        });
    }

    private WriteTask enqueue(Runnable handler) {
        try {
            WriteTask task = new WriteTask(handler);
            tasks.put(task);
            return task;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void process() {
        while (!closed.get()) {
            try {
                WriteTask task = tasks.poll(1, TimeUnit.SECONDS);
                if (task == null || task.isCancelled()) {
                    continue;
                }

                do {
                    try {
                        pending.add(task);
                        task.handler.run();
                    } catch (Exception e) {
                        e.printStackTrace();
                        pending.remove(pending.size() - 1); //not really needed, but just for correctness
                        task.completeExceptionally(e);
                    }
                    if (writer.isFull()) {
                        flushBatch();
                    }
                    task = poolNext();

                } while (task != null);

                if (!writer.isEmpty()) {
                    flushBatch();
                }
            } catch (Exception e) {
                e.printStackTrace();
                closed.set(true);
            }
        }
        System.out.println("Write thread closed");
    }

    private void flushBatch() {
        writer.flushBatch();
        for (WriteTask task : pending) {
            task.complete(null);
        }
        pending.clear();
    }


    private WriteTask poolNext() throws InterruptedException {
        if (poolWait <= 0) {
            return tasks.poll();
        }
        return tasks.poll(poolWait, TimeUnit.MILLISECONDS);
    }

    //API only, not to be used internally
    public void commit() {
        WriteTask task = enqueue(this::flushBatch);
        task.join();
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        closed.set(true);
        commit();
    }

}
