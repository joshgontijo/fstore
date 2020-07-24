package io.joshworks.ilog.record;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * NOT THREAD SAFE
 * Use with a {@link ThreadLocal} for multiple threads
 */
public class StripedBufferPool implements StripedPool {

    private final long capacityInBytes;
    private final boolean direct;

    private final AtomicLong totalAllocated = new AtomicLong();
    private final TreeMap<Integer, Queue<ByteBuffer>> pools = new TreeMap<>();

    //stats
    private final Map<Integer, AtomicInteger> count = new HashMap<>();
    private final Map<Integer, AtomicInteger> allocated = new HashMap<>();
    private final Map<Integer, AtomicInteger> inUse = new HashMap<>();

    public static StripedPool create(int capacityInBytes, boolean direct, Integer... stripes) {
        return new StripedBufferPool(capacityInBytes, direct, new HashSet<>(Arrays.asList(stripes)));
    }

    private StripedBufferPool(int capacityInBytes, boolean direct, Set<Integer> stripes) {
        this.capacityInBytes = capacityInBytes;
        this.direct = direct;

        for (int stripe : stripes) {
            if (stripe <= 0) {
                throw new IllegalArgumentException("Stripe must great than zero");
            }
            pools.put(stripe, new ArrayDeque<>());

            count.put(stripe, new AtomicInteger());
            allocated.put(stripe, new AtomicInteger());
            inUse.put(stripe, new AtomicInteger());
        }
    }

    @Override
    public ByteBuffer allocate(int size) {
        var entry = pools.ceilingEntry(size);

        if (entry == null) {
            throw new IllegalArgumentException("Cannot allocated buffer for size " + size + " available stripes: " + pools.keySet());
        }
        var queue = entry.getValue();
        int stripeSize = entry.getKey();
        ByteBuffer buffer = queue.poll();

        buffer = buffer == null ? allocateBuffer(stripeSize) : buffer;
        inUse.get(stripeSize).incrementAndGet();
        return buffer.clear().limit(size);
    }

    @Override
    public void free(ByteBuffer buffer) {
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
        if (totalAllocated.get() + size > capacityInBytes) {
            System.err.println("BUFFER POOL CAPACITY EXCEEDED: " + totalAllocated.get() + ", CAPACITY: " + capacityInBytes);
        }
        ByteBuffer buffer = Buffers.allocate(size, direct);

        count.get(size).incrementAndGet();
        allocated.get(size).addAndGet(size);
        totalAllocated.addAndGet(size);

        return buffer;
    }

    @Override
    public void close() {
        pools.clear();
    }
}
