package io.joshworks.es2;

import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.core.util.Threads;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.failedFuture;

class BatchWriter implements Closeable {

    private final long poolTime;
    private final long batchTimeout;

    private final BlockingQueue<WriteTask> tasks;
    private final EventStore store;
    private final ByteBuffer[] writeItems;
    private final WriteTask[] inProgress;
    private boolean closed;
    private final CountDownLatch closeLatch = new CountDownLatch(1);


    BatchWriter(EventStore store, long poolTime, long batchTimeout, int maxItems) {
        this.store = store;
        this.poolTime = poolTime;
        this.batchTimeout = batchTimeout;
        this.tasks = new ArrayBlockingQueue<>(maxItems);
        this.writeItems = new ByteBuffer[maxItems];
        this.inProgress = new WriteTask[maxItems];
        Threads.spawn("batch-writer", this::flushTask);
    }

    CompletableFuture<Integer> write(ByteBuffer event) {
        if (closed) {
            return failedFuture(new RuntimeException("Closed writer"));
        }

        var task = new WriteTask(event);
        try {
            tasks.put(task);
            return task;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to queue write task");
        }
    }

    private void flushTask() {
        while (!closed) {
            Map<Long, Integer> cachedVersions = new HashMap<>();
            try {
                WriteTask task;
                int items = 0;
                long poolStart = System.currentTimeMillis();
                do {
                    task = tasks.poll(poolTime, TimeUnit.MILLISECONDS);
                    if (task == null) {
                        continue;
                    }


                    //--- VERSION CHECK
                    var event = task.event;
                    int eventVersion = Event.version(event);
                    long stream = Event.stream(event);
                    Integer cachedVersion = cachedVersions.get(stream);

                    int currVersion = cachedVersion == null ? store.version(stream) : cachedVersion;
                    int nextVersion = currVersion + 1;
                    if (eventVersion != -1 && eventVersion != nextVersion) {
                        task.completeExceptionally(new VersionMismatch(stream, eventVersion, currVersion));
                        continue;
                    }
                    Event.writeVersion(event, nextVersion);
                    Event.writeTimestamp(event, System.currentTimeMillis());

                    cachedVersions.put(stream, nextVersion);
                    task.version = nextVersion;
                    //----

                    inProgress[items] = task;
                    writeItems[items] = event;
                    items++;
                } while (task != null && items < writeItems.length && System.currentTimeMillis() - poolStart < batchTimeout);

                if (items > 0) {
//                    System.out.println("Writting " + items + " items, batchTime: " + (System.currentTimeMillis() - poolStart) + "ms");
                    var watch = TimeWatch.start();
                    flush(inProgress);
//                    System.out.println("Flushed " + items + "in " + watch.elapsed() + "ms");
                    cachedVersions.clear();
                }


            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closed = true;
            } catch (Exception e) {
                System.err.println("INTERNAL ERROR WHILE WRITTING");
                e.printStackTrace();
                closed = true;
                System.exit(-1);
            }
        }

        //drain queued tasks
        WriteTask task;
        while ((task = tasks.poll()) != null) {
            task.completeExceptionally(new RuntimeException("Closed writer"));
        }

        closeLatch.countDown();
    }

//    private boolean checkVersion(WriteTask task) {
//        int eventVersion = Event.version(task.event);
//        long stream = Event.stream(task.event);
//
//        int currVersion = store.version(stream);
//        int nextVersion = currVersion + 1;
//        if (eventVersion != -1 && eventVersion != nextVersion) {
//            task.completeExceptionally(new VersionMismatch(stream, eventVersion, currVersion));
//            return false;
//        }
//        return true;
//    }

    private void flush(WriteTask[] tasks) {
        store.tlog.append(writeItems);
        for (int i = 0; i < tasks.length; i++) {
            var event = writeItems[i];
            var task = tasks[i];
            assert task.event == event;

            event.flip();
            if (!store.memTable.add(event)) {
                store.roll();
                store.memTable.add(event);
            }
            task.complete(task.version);
        }
    }


    @Override
    public void close() {
        try {
            this.closed = true;
            closeLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Failed to close writer");
        }
    }
}
