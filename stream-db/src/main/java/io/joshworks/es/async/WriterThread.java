package io.joshworks.es.async;

import io.joshworks.es.Event;
import io.joshworks.es.StreamHasher;
import io.joshworks.es.index.Index;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class WriterThread {

    private final Thread thread = new Thread(this::process, "writer");
    private final BlockingQueue<WriteTask> tasks = new ArrayBlockingQueue<>(100);
    private final AtomicBoolean closed = new AtomicBoolean();

    private final Log log;
    private final Index index;
    private final ByteBuffer writeBuffer;
    private final long[] logPositions;
    private final WriteTask[] processingTasks;
    private final long poolWait;
    private final int maxEntries;
    private int entries;

    private final AtomicLong sequence = new AtomicLong();


    public WriterThread(Log log, Index index, int maxEntries, int bufferSize, long poolWait) {
        this.log = log;
        this.index = index;
        this.maxEntries = maxEntries;
        this.writeBuffer = ByteBuffer.allocate(bufferSize);
        this.logPositions = new long[maxEntries];
        this.processingTasks = new WriteTask[maxEntries];
        this.poolWait = poolWait;
    }

    public CompletableFuture<TaskResult> submit(ByteBuffer rec) {
        try {
            WriteTask task = new WriteTask(rec);
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

                long seq = sequence.get();
                int segIdx = log.segmentIdx();
                long logPos = log.segmentPosition();
                do {
                    ByteBuffer data = task.data;

                    int eventSize = Event.sizeOf(data);
                    if (eventSize > writeBuffer.capacity()) {
                        System.err.println("Event too large");
                    }
                    if (eventSize > writeBuffer.remaining() || entries >= maxEntries) {
                        writeBuffer.flip();
                        long logPos = log.append(eventData);

                        entries = 0;

                        Arrays.fill(logPositions, 0);
                        Arrays.fill(processingTasks, null);
                    }
                    Buffers.copy(data, writeBuffer);
                    logPositions[entries] = Log.toSegmentedPosition(segIdx, logPos);
                    entries++;
                    logPos += eventSize;


                    index.append(stream, version, eventSize, logPos);

                    task = tasks.poll(poolWait, TimeUnit.MILLISECONDS);

                } while (task != null);


                writeBuffer.flip();
                log.append(writeBuffer);
                task.complete(new TaskResult(true, "completed"));

            } catch (Exception e) {
                e.printStackTrace();
                closed.set(true);
            }
        }
    }

    private void serialize(WriteEvent rec) {
        long seq = sequence.incrementAndGet();
        ByteBuffer evData = Event.create(seq, StreamHasher.hash(rec.stream), rec.version, ByteBuffer.wrap(rec.data));
        Buffers.copy(evData, writeBuffer);
    }

    public void start() {

    }

    public void shutdown() {
        closed.set(true);
    }

}
