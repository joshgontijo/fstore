package io.joshworks.ilog.polled;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Function;

public class ObjectPool<T> {
    private final Queue<T> items;
    private final Function<ObjectPool<T>, T> supplier;

    public ObjectPool(int items, Function<ObjectPool<T>, T> supplier) {
        this.items = new ArrayBlockingQueue<>(items);
        this.supplier = supplier;
    }

    public T allocate() {
        T poll = items.poll();
        if (poll == null) {
            return supplier.apply(this);
        }
        return poll;
    }

    public void release(T pooled) {
        items.offer(pooled);
    }


}