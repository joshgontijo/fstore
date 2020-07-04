package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class StripedBufferPool {

    private final long maxSizeInBytes;
    private final boolean direct;

    private final AtomicLong totalAllocated = new AtomicLong();
    private final TreeMap<Integer, Queue<ByteBuffer>> pools = new TreeMap<>();

    //stats
    private final Map<Integer, AtomicInteger> count = new HashMap<>();
    private final Map<Integer, AtomicInteger> allocated = new HashMap<>();
    private final Map<Integer, AtomicInteger> inUse = new HashMap<>();

    public StripedBufferPool(int maxSizeInBytes, boolean direct, Set<Integer> stripes) {
        this.direct = direct;
        this.maxSizeInBytes = maxSizeInBytes;

        for (int stripe : stripes) {
            pools.put(stripe, new ArrayDeque<>());
            count.put(stripe, new AtomicInteger());
        }
    }


    ByteBuffer allocate(int size) {
        var entry = pools.ceilingEntry(size);
        if (entry == null) {
            throw new IllegalArgumentException("Requested buffer size: " + size + " is greater than allowed: " + pools.lastKey());
        }
        var queue = entry.getValue();
        int stripeSize = entry.getKey();
        ByteBuffer buffer = queue.poll();


        buffer = buffer == null ? allocateBuffer(stripeSize) : buffer;
        inUse.get(size).incrementAndGet();
        return buffer;
    }

    void free(ByteBuffer buffer) {
        if (buffer == null) {
            throw new IllegalArgumentException("Buffer must not be null");
        }
        buffer.clear();

        int bufferSize = buffer.capacity();
        var entry = pools.ceilingEntry(bufferSize);
        if (entry == null || entry.getKey() != bufferSize) {
            throw new IllegalArgumentException("Invalid buffer");
        }

        entry.getValue().offer(buffer);

        inUse.get(bufferSize).decrementAndGet();
    }


    private ByteBuffer allocateBuffer(int size) {
        if (totalAllocated.get() + size > maxSizeInBytes) {
            System.err.println("BUFFER POOL CAPACITY EXCEEDED: " + totalAllocated.get() + ", CAPACITY: " + maxSizeInBytes);
        }
        ByteBuffer buffer = Buffers.allocate(size, direct);

        count.get(size).incrementAndGet();
        allocated.get(size).addAndGet(size);
        totalAllocated.addAndGet(size);

        return buffer;
    }

}
