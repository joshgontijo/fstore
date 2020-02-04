package io.joshworks.ilog.pooled;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class ObjectPool {

    private final Map<Class<? extends Pooled>, Pool<? extends Pooled>> pools = new ConcurrentHashMap<>();

    public <T extends Pooled> void create(Class<T> type, int maxItems, Function<Pool<T>, T> supplier) {
        pools.putIfAbsent(type, new Pool<>(maxItems, supplier));
    }

    public <T extends Pooled> T allocate(Class<T> type) {
        return (T) pools.get(type).allocate();
    }


    public static class Pool<T extends Pooled> {
        private final Queue<T> items;
        private final Function<Pool<T>, T> supplier;

        private Pool(int items, Function<Pool<T>, T> supplier) {
            this.items = new ArrayBlockingQueue<>(items);
            this.supplier = supplier;
        }


        private T allocate() {
            T poll = items.poll();
            if (poll == null) {
                return supplier.apply(this);
            }
            return poll;
        }

        void release(Pooled pooled) {
            items.offer((T) pooled);
        }
    }


}
