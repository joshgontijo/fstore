package io.joshworks.es.async;

import io.joshworks.es.Event;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
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
    private final ByteBuffer writeBuffer;
    private final long poolWait;
    private final int maxEntries;
    private final List<WriteTask> processing = new ArrayList<>();

    private final AtomicLong sequence = new AtomicLong();


    public WriterThread(Log log, int maxEntries, int bufferSize, long poolWait) {
        this.log = log;
        this.maxEntries = maxEntries;
        this.writeBuffer = ByteBuffer.allocate(bufferSize);
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
                    long stream = Event.stream(data);
                    int version = Event.version(data);
                    Event.writeSequence(data, data.position(), seq);
                    Event.writeChecksum(data, data.position());

                    if (eventSize > writeBuffer.capacity()) {
                        System.err.println("Event too large");
                    }
                    if (eventSize > writeBuffer.remaining() || processing.size() >= maxEntries) {
                        write(seq);
                        segIdx = log.segmentIdx();
                        logPos = log.segmentPosition();
                    }
                    int copied = Buffers.copy(data, writeBuffer);
                    assert copied == eventSize;
                    task.result = prepare(seq, segIdx, logPos, eventSize, stream, version);

                    logPos += eventSize;
                    seq++;

                    processing.add(task);

                    task = tasks.poll(poolWait, TimeUnit.MILLISECONDS);

                } while (task != null);

                if (!processing.isEmpty()) {
                    write(seq);
                }

            } catch (Exception e) {
                e.printStackTrace();
                closed.set(true);
            }

            System.out.println("Write thread closed");

        }
    }

    private TaskResult prepare(long seq, int segIdx, long logPos, int eventSize, long stream, int version) {
        long logAddress = Log.toSegmentedPosition(segIdx, logPos);
        return new TaskResult(logAddress, eventSize, stream, version, seq);
    }

    private void write(long seq) {
        writeBuffer.flip();
        log.append(writeBuffer);
        writeBuffer.compact();
        sequence.set(seq);
        complete();
    }

    private void complete() {
        System.out.println("Completing: " + processing.size() + " tasks");
        for (WriteTask task : processing) {
            task.complete(task.result);
        }
        processing.clear();

    }

    public void start() {
        thread.start();
    }

    public void shutdown() {
        closed.set(true);
    }

}
