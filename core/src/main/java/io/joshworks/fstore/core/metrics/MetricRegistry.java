package io.joshworks.fstore.core.metrics;

import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class MetricRegistry {

    private static final Map<String, Metrics> table = new ConcurrentHashMap<>();

    public static Metrics create(String type) {
        return table.compute(type, (k, v) -> {
            v = getOrCreate(type, v);
            return v;
        });
    }

    private static Metrics getOrCreate(String type, Metrics v) {
        if (v == null) {
            try {
                ObjectName objectName = new ObjectName("fstore:type=" + type);
                v = new Metrics(type, objectName, MetricRegistry::remove);
                ManagementFactory.getPlatformMBeanServer().registerMBean(v, objectName);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return v;
    }

    public static void monitor(String name, ThreadPoolExecutor pool) {
        Metrics metrics = MetricRegistry.create(name + ".threadPool");
        metrics.register("activeCount", () -> (long) pool.getActiveCount());
        metrics.register("corePoolSize", () -> (long) pool.getCorePoolSize());
        metrics.register("poolSize", () -> (long) pool.getPoolSize());
        metrics.register("largestPoolSize", () -> (long) pool.getLargestPoolSize());
        metrics.register("maximumPoolSize", () -> (long) pool.getMaximumPoolSize());
        metrics.register("taskCount", pool::getTaskCount);
        metrics.register("queuedTasks", () -> pool.getTaskCount() - pool.getCompletedTaskCount() - pool.getActiveCount());
        metrics.register("completedTasks", pool::getCompletedTaskCount);
        //TODO remove pool from JMX / table to avoid memory leak, ideally by extending ThreadPoolExecutor
    }

    public static void remove(String type) {
        table.remove(type);
    }

    public static void clear() {
        table.values().forEach(Metrics::close);
    }


}
