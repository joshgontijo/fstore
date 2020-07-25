package io.joshworks.es.writer;

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

    private final long poolWait;
    private final BatchingWriter writer;

    public StoreWriter(Log log, Index index, int maxEntries, int bufferSize, long poolWait) {
        this.poolWait = poolWait;
        this.writer = new BatchingWriter(log, index, maxEntries, bufferSize);
        this.tasks = new ArrayBlockingQueue<>(maxEntries);
    }

    public WriteTask submit(Consumer<BatchingWriter> handler) {
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
                WriteTask task = tasks.poll(200, TimeUnit.MILLISECONDS);
                if (task == null || task.isCancelled()) {
                    continue;
                }

                do {
                    try {
                        writer.prepare(task);
                        task.handler.accept(writer);
                        writer.complete();
                    } catch (Exception e) {
                        e.printStackTrace();
                        writer.rollback(e);
                    }
                    if (writer.isFull()) {
                        writer.commit();
                    }
                    task = poolNext();

                } while (task != null);

                if (!writer.isEmpty()) {
                    writer.commit();
                }
            } catch (Exception e) {
                e.printStackTrace();
                closed.set(true);
            }
        }
        System.out.println("Write thread closed");
    }

    private WriteTask poolNext() throws InterruptedException {
        if (poolWait <= 0) {
            return tasks.poll();
        }
        return tasks.poll(poolWait, TimeUnit.MILLISECONDS);
    }

    //API only, not to be used internally
    public void commit() {
        WriteTask task = submit(BatchingWriter::commit);
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
