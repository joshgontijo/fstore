package io.joshworks.es.writer;

import io.joshworks.es.index.Index;
import io.joshworks.es.log.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class WriterThread {

    private final Thread thread = new Thread(this::process, "writer");
    private final AtomicBoolean closed = new AtomicBoolean();

    private final BlockingQueue<WriteTask> tasks;
    private final List<WriteTask> staging;

    private final long poolWait;
    private final StoreWriter writer;

    public WriterThread(Log log, Index index, int maxEntries, int bufferSize, long poolWait) {
        this.poolWait = poolWait;
        this.writer = new StoreWriter(log, index, maxEntries, bufferSize);
        this.tasks = new ArrayBlockingQueue<>(maxEntries);
        this.staging = new ArrayList<>(maxEntries);
    }

    public CompletableFuture<Void> submit(Consumer<StoreWriter> handler) {
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
                        //TODO pass handler to writer or something so it can report completion
                        writer.prepare();
                        task.handler.accept(writer);
                        writer.complete();
                        staging.add(task);
                    } catch (Exception e) {
                        e.printStackTrace();
                        writer.rollback();
                        task.completeExceptionally(e);
                    }
                    if (writer.isFull()) {
                        commit();
                    }
                    task = poolNext();

                } while (task != null);

                if (!writer.isEmpty()) {
                    commit();
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

    private void commit() {
        writer.commit();
        for (WriteTask task : staging) {
            task.complete(null);
        }
        staging.clear();
    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        commit();
        closed.set(true);
    }

}
