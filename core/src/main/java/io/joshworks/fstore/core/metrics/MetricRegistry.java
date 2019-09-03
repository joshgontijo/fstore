package io.joshworks.fstore.core.metrics;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class MetricRegistry {

    private static final Map<String, Metrics> table = new ConcurrentHashMap<>();

    public static Metrics create(String type) {
        return table.compute(type, (k, v) -> {
            v = getOrCreate(type, v);
            return v;
        });
    }

    public static void update(String type, String name) {
        table.compute(name, (k, v) -> {
            v = getOrCreate(type, v);
            v.update(name);
            return v;
        });
    }

    public static void set(String type, String name, long value) {
        table.compute(name, (k, v) -> {
            getOrCreate(type, v).set(name, value);
            return v;
        });
    }

    public static void register(String type, String name, Supplier<Long> supplier) {
        table.compute(name, (k, v) -> {
            getOrCreate(type, v).register(name, supplier);
            return v;
        });
    }

    public static void register(String type, String name, Consumer<AtomicLong> supplier) {
        table.compute(name, (k, v) -> {
            getOrCreate(type, v).register(name, supplier);
            return v;
        });
    }

    private static Metrics getOrCreate(String type, Metrics v) {
        if (v == null) {
            v = new Metrics(type);
        }
        return v;
    }

    public static void monitor(String name, ThreadPoolExecutor pool) {
        Metrics metrics = MetricRegistry.create(name);
        metrics.register("activeCount", () -> (long) pool.getActiveCount());
        metrics.register("corePoolSize", () -> (long) pool.getCorePoolSize());
        metrics.register("poolSize", () -> (long) pool.getPoolSize());
        metrics.register("largestPoolSize", () -> (long) pool.getLargestPoolSize());
        metrics.register("maximumPoolSize", () -> (long) pool.getMaximumPoolSize());
        metrics.register("taskCount", pool::getTaskCount);
        metrics.register("queuedTasks", () -> pool.getTaskCount() - pool.getCompletedTaskCount() - pool.getActiveCount());
        metrics.register("completedTasks", pool::getCompletedTaskCount);
        //TODO remove pool from JMX / table to avoid memory leak
    }

    public static void remove(String type) {
        table.remove(type);
    }

    public static void clear() {
        table.values().forEach(Metrics::close);
    }


}
