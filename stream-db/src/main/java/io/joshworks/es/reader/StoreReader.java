package io.joshworks.es.reader;

import io.joshworks.es.StoreLock;
import io.joshworks.es.index.Index;
import io.joshworks.es.log.Log;
import io.joshworks.fstore.core.util.Threads;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

public class StoreReader implements Closeable {

    //not static, multiple stores can have multiple readers
    private final ThreadLocal<QueryPlanner> plannerCache;

    private final StoreLock storeLock;

    private final int maxEntries;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicLong reading = new AtomicLong(); //current number of reads

    //metrics
    private final AtomicLong reads = new AtomicLong(); //current number of reads
    private final AtomicLong bytesRead = new AtomicLong(); //current number of reads

    public StoreReader(Log log, Index index, StoreLock storeLock, int maxEntries, int pageBufferSize) {
        this.storeLock = storeLock;
        this.maxEntries = maxEntries;
        this.plannerCache = ThreadLocal.withInitial(() -> new QueryPlanner(index, log, pageBufferSize));
    }

    public int get(long stream, int version, ByteBuffer dst) {
        if (closed.get()) {
            return 0;
        }

        reading.incrementAndGet();
        reads.incrementAndGet();
        Lock lock = storeLock.readLock();
        try {
            int bytes = getInternal(stream, version, dst);
            bytesRead.addAndGet(bytes);
            return bytes;
        } finally {
            lock.unlock();
            reading.decrementAndGet();
        }

    }

    private int getInternal(long stream, int version, ByteBuffer dst) {
        QueryPlanner planner = plannerCache.get();
        boolean success = planner.prepare(stream, version, maxEntries);
        if (!success) {
            return 0;
        }
        return planner.execute(dst);
    }

    //awaits all reads to complete
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        while (reading.get() > 0) {
            Threads.sleep(1000);
        }

    }
}
