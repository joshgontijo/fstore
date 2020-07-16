package io.joshworks.fstore.core.util;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

public class ObjectPool<T> {
    //TODO replace with ArrayBlockingQueue
    private final Queue<T> items = new ConcurrentLinkedQueue<>();
    private final Function<ObjectPool<T>, T> supplier;

    public ObjectPool(Function<ObjectPool<T>, T> supplier) {
        this.supplier = supplier;
    }

    public T allocate() {
        T poll = items.poll();
        if (poll == null) {
            poll =  supplier.apply(this);
        }
        return poll;
    }

    public void release(T pooled) {
        if (pooled != null) {
            items.offer(pooled);
        }
    }

    public void clear() {
        items.clear();
    }
}