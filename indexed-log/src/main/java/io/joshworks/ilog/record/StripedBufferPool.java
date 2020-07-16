package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StripedBufferPool {

    private final long maxSize;
    private final boolean direct;

    private final AtomicLong totalAllocated = new AtomicLong();
    private final TreeMap<Integer, Queue<ByteBuffer>> pools = new TreeMap<>();

    //stats
    private final Map<Integer, AtomicInteger> count = new HashMap<>();
    private final Map<Integer, AtomicInteger> allocated = new HashMap<>();
    private final Map<Integer, AtomicInteger> inUse = new HashMap<>();

    public StripedBufferPool(int maxSize, boolean direct, Set<Integer> stripes) {
        this.maxSize = maxSize;
        this.direct = direct;

        stripes = new HashSet<>(stripes);
        stripes.add(maxSize);

        for (int stripe : stripes) {
            //TODO replace with ArrayBlockingQueue
            pools.put(stripe, new ConcurrentLinkedQueue<>());

            count.put(stripe, new AtomicInteger());
            allocated.put(stripe, new AtomicInteger());
            inUse.put(stripe, new AtomicInteger());
        }
    }

    ByteBuffer allocate(int size) {
        var entry = pools.ceilingEntry(size);
        assert entry != null;
        var queue = entry.getValue();
        int stripeSize = entry.getKey();
        ByteBuffer buffer = queue.poll();


        buffer = buffer == null ? allocateBuffer(stripeSize) : buffer;
        inUse.get(stripeSize).incrementAndGet();
        return buffer.clear().limit(size);
    }

    void free(ByteBuffer buffer) {
        assert buffer != null;
        buffer.clear();

        int bufferSize = buffer.capacity();
        var entry = pools.ceilingEntry(bufferSize);

        assert entry != null;
        assert entry.getKey() == bufferSize;

        entry.getValue().offer(buffer);

        inUse.get(bufferSize).decrementAndGet();
    }


    private ByteBuffer allocateBuffer(int size) {
        if (totalAllocated.get() + size > maxSize) {
//            System.err.println("BUFFER POOL CAPACITY EXCEEDED: " + totalAllocated.get() + ", CAPACITY: " + maxSize);
        }
        ByteBuffer buffer = Buffers.allocate(size, direct);

        count.get(size).incrementAndGet();
        allocated.get(size).addAndGet(size);
        totalAllocated.addAndGet(size);

        return buffer;
    }

    public void close() {
        pools.clear();
    }
}
