package io.joshworks.es.writer;

import io.joshworks.es.events.SystemStreams;
import io.joshworks.es.events.WriteEvent;
import io.joshworks.es.index.Index;
import io.joshworks.es.log.Log;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class StoreWriter {

    private final Thread thread = new Thread(this::process, "writer");
    private final AtomicBoolean closed = new AtomicBoolean();

    private final BlockingQueue<WriteTask> tasks;

    private final Log log;
    private final Index index;
    private final long poolWait;
    private final BatchingWriter writer;

    public StoreWriter(Log log, Index index, int maxEntries, int bufferSize, long poolWait) {
        this.log = log;
        this.index = index;
        this.poolWait = poolWait;
        this.writer = new BatchingWriter(log, index, maxEntries, bufferSize);
        this.tasks = new ArrayBlockingQueue<>(maxEntries);
    }

    //No need for StoreLock here as it will always be appending to the head
    public WriteTask enqueue(WriteEvent event) {
        return enqueue(w -> w.append(event));
    }

    //No need for StoreLock here as it will always be appending to the head
    public WriteTask enqueue(Consumer<BatchingWriter> handler) {
        try {
            WriteTask task = new WriteTask(handler);
            tasks.put(task);
            return task;
        } catch (InterruptedException e) {
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
                        handleTask(task);
                    } catch (Exception e) {
                        e.printStackTrace();
                        writer.rollback(e);
                    }
                    if (writer.isFull()) {
                        commitInternal();
                    }
                    task = poolNext();

                } while (task != null);

                if (!writer.isEmpty()) {
                    commitInternal();
                }
            } catch (Exception e) {
                e.printStackTrace();
                closed.set(true);
            }
        }
        System.out.println("Write thread closed");
    }

    private void handleTask(WriteTask task) {
        writer.prepare(task);
        task.handler.accept(writer);
        writer.complete();
    }

    private void tryFlush() {
        if(index.isFull()) {
            writer.append(SystemStreams.indexFlush());
            writer.commit();
        }
    }

    private void commitInternal() {
        try {
            writer.commit();
        } catch (Exception e) {
            //TODO need to have some sort of abort that will notify all tasks otherwise they might hang forever
            System.err.println("[FATAL] Failed to commit transactions, stopping writer thread");
            e.printStackTrace();
            closed.set(true);
        }
    }

    private WriteTask poolNext() throws InterruptedException {
        if (poolWait <= 0) {
            return tasks.poll();
        }
        return tasks.poll(poolWait, TimeUnit.MILLISECONDS);
    }

    //API only, not to be used internally
    public void commit() {
        WriteTask task = enqueue(BatchingWriter::commit);
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
