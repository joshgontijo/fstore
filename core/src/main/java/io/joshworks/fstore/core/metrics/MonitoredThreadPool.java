package io.joshworks.fstore.core.metrics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class MonitoredThreadPool implements ExecutorService {

    private final Metrics metrics = new Metrics();
    private final String metricsKey;
    private final ThreadPoolExecutor delegate;
    private final AtomicLong totalWaitTime = new AtomicLong();
    private final AtomicLong processingTime = new AtomicLong();
    private final AtomicLong lastWaitTime = new AtomicLong();
    private final AtomicLong lastProcessTime = new AtomicLong();

    public MonitoredThreadPool(String name, ThreadPoolExecutor delegate) {
        this.delegate = delegate;
        this.metricsKey = MetricRegistry.register(Map.of("type", "threadPools", "pollName", name), this::metrics);
    }


    private Metrics metrics() {
        long totalWaitTime = this.totalWaitTime.get();
        long totalProcessingTime = this.processingTime.get();
        long completedTaskCount = delegate.getCompletedTaskCount();

        metrics.set("activeCount", delegate.getActiveCount());
        metrics.set("corePoolSize", delegate.getCorePoolSize());
        metrics.set("poolSize", delegate.getPoolSize());
        metrics.set("largestPoolSize", delegate.getLargestPoolSize());
        metrics.set("maximumPoolSize", delegate.getMaximumPoolSize());
        metrics.set("taskCount", delegate.getTaskCount());
        metrics.set("queuedTasks", delegate.getTaskCount() - completedTaskCount - delegate.getActiveCount());
        metrics.set("completedTasks", completedTaskCount);
        metrics.set("totalWaitTime", totalWaitTime);
        metrics.set("avgWaitTime", totalWaitTime > 0 ? totalWaitTime / completedTaskCount : totalWaitTime);
        metrics.set("totalProcessingTime", totalProcessingTime);
        metrics.set("avgProcessingTime", totalProcessingTime > 0 ? totalProcessingTime / completedTaskCount : totalProcessingTime);
        metrics.set("lastWaitTime", lastWaitTime.get());
        return metrics;
    }

    @Override
    public void shutdown() {
        MetricRegistry.remove(metricsKey);
        delegate.shutdown();

    }

    @Override
    public List<Runnable> shutdownNow() {
        MetricRegistry.remove(metricsKey);
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(timedCallable(task));
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(timedRunnable(task), result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate.submit(timedRunnable(task));
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(timedRunnable(command));
    }

    private Runnable timedRunnable(Runnable delegate) {
        long submitted = System.currentTimeMillis();
        return () -> {
            long start = System.currentTimeMillis();
            long waitTime = start - submitted;
            totalWaitTime.addAndGet(waitTime);
            lastWaitTime.set(waitTime);
            delegate.run();
            long end = System.currentTimeMillis();
            long processTime = end - start;
            processingTime.addAndGet(processTime);
            lastProcessTime.set(processTime);
        };
    }

    private <T> Callable<T> timedCallable(Callable<T> delegate) {
        long submitted = System.currentTimeMillis();
        return () -> {
            long start = System.currentTimeMillis();
            long waitTime = start - submitted;
            totalWaitTime.addAndGet(waitTime);
            lastWaitTime.set(waitTime);
            T returnObj = delegate.call();
            long end = System.currentTimeMillis();
            long processTime = end - start;
            processingTime.addAndGet(processTime);
            lastProcessTime.set(processTime);
            return returnObj;
        };
    }

}
