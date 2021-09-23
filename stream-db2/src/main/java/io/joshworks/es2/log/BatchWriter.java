package io.joshworks.es2.log;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.joshworks.es2.log.TLog.ENTRY_PART_COUNT;
import static io.joshworks.es2.log.TLog.FOOTER_SIZE;
import static io.joshworks.es2.log.TLog.HEADER_SIZE;

class BatchWriter {

    private boolean shutdown;
    private int batchEntries;
    private long lastFlushed;

    private final TLog log;
    private final long maxFlushDelay;
    private final Thread worker;
    private final BlockingQueue<WriteTask> tasks;
    private final ByteBuffer[] buffers;

    BatchWriter(TLog log, int maxEntries, long maxFlushDelay) {
        this.log = log;
        this.maxFlushDelay = maxFlushDelay;
        this.buffers = createWriteBuffers(maxEntries); //3 = header + data + footer
        this.tasks = new ArrayBlockingQueue<>(maxEntries);

        worker = new Thread(this::run);
        worker.setName("log-writer");
    }

    CompletableFuture<Long> submit(ByteBuffer data, long sequence, Type type) {
        if (shutdown) {
            return CompletableFuture.completedFuture(-1L);
        }
        try {
            var task = new WriteTask(data, sequence, type);
            tasks.put(task);
            return task;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Thread interrupted", e);
        }
    }

    void shutdown() {
        shutdown = true;
    }

    private void run() {
        while (!shutdown || !tasks.isEmpty()) {
            try {
                WriteTask task = tasks.poll(100, TimeUnit.MILLISECONDS);

                if (task != null && !task.isCancelled()) {
                    composeLogEntry(task, batchEntries);
                    batchEntries++;
                }

                long now = System.currentTimeMillis();
                if ((now - lastFlushed > maxFlushDelay && batchEntries > 0) || batchEntries >= buffers.length) {
                    //TODO write
                    lastFlushed = now;
                }

            } catch (Exception e) {
                e.printStackTrace();
                shutdown = true;
            }
        }
    }


    private static ByteBuffer[] createWriteBuffers(int entries) {
        var items = new ByteBuffer[ENTRY_PART_COUNT * entries];
        for (var i = 0; i < items.length; i += ENTRY_PART_COUNT) {
            items[i] = Buffers.allocate(HEADER_SIZE, false); //header
            items[i + 1] = null; //data
            items[i + 2] = Buffers.allocate(FOOTER_SIZE, false); // footer
        }
        return items;
    }

    private void composeLogEntry(WriteTask task, int buffOffset) {

        var data = task.data;
        var sequence = task.sequence;
        var type = task.type;

        int eventSize = data.remaining();
        int recSize = eventSize + HEADER_SIZE + FOOTER_SIZE;
        buffers[buffOffset]
                .clear()
                .putInt(recSize)
                .putInt(ByteBufferChecksum.crc32(data, data.position(), eventSize))
                .putLong(sequence)
                .put(type.i)
                .flip();

        buffers[buffOffset + 1] = data;

        buffers[buffOffset + 2]
                .clear()
                .putInt(recSize)
                .flip();
    }


}
