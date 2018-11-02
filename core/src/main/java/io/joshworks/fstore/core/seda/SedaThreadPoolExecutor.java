package io.joshworks.fstore.core.seda;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SedaThreadPoolExecutor extends ThreadPoolExecutor {

    private final int queueSize;
    private final int queueHighBound;
    private final RejectedExecutionHandlerWrapper rejectionHandler;
    private final AtomicLong totalTime = new AtomicLong();
    private final AtomicLong queueTime = new AtomicLong();
    private final AtomicLong totalExecutions = new AtomicLong();
    private final TimeWatch timer = TimeWatch.start();
    private final Semaphore semaphore;

    private SedaThreadPoolExecutor(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, SedaThreadFactory threadFactory, int queueSize, int queueHighBound, RejectedExecutionHandlerWrapper rejectionHandler) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new LinkedBlockingQueue<>(), threadFactory, rejectionHandler);
        this.queueSize = queueSize;
        this.semaphore = new Semaphore(queueSize);
        this.queueHighBound = queueHighBound;
        this.rejectionHandler = rejectionHandler;
    }

    public static SedaThreadPoolExecutor create(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, int queueSize, int queueHighBound, RejectedExecutionHandler handler, boolean blockWhenFull) {
        SedaThreadFactory threadFactory = new SedaThreadFactory(name);
        RejectedExecutionHandlerWrapper rejectionHandler = new RejectedExecutionHandlerWrapper(handler);
        return new SedaThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, unit, threadFactory, queue, rejectionHandler);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        SedaTask tr = (SedaTask) r;
        queueTime.addAndGet(System.currentTimeMillis() - tr.queuedTime);

        super.beforeExecute(t, r);
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        totalExecutions.incrementAndGet();
        SedaTask task = (SedaTask) r;
        totalTime.addAndGet(task.executionTime);
    }

    @Override
    public void execute(final Runnable command) {
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }

        SedaTask task = (SedaTask) command;
        task.semaphore = semaphore;
        super.execute(task);
    }

    long totalTime() {
        return totalTime.get();
    }

    double averageExecutionTime() {
        long totalTasks = totalExecutions.get();
        return (totalTasks == 0) ? 0 : totalTime.get() / (double) totalTasks;
    }

    double averageQueueTime() {
        long totalTasks = totalExecutions.get();
        return (totalTasks == 0) ? 0 : queueTime.get() / (double) totalTasks;
    }

    long rejectedTasks() {
        return rejectionHandler.rejectedTasksCount();
    }

    private static class RejectedExecutionHandlerWrapper implements RejectedExecutionHandler {

        private final RejectedExecutionHandler delegate;
        private final AtomicLong rejectedCount = new AtomicLong();

        RejectedExecutionHandlerWrapper(RejectedExecutionHandler handler) {
            this.delegate = handler;
        }

        @Override
        public void rejectedExecution(Runnable task, ThreadPoolExecutor executor) {
            rejectedCount.incrementAndGet();
            delegate.rejectedExecution(task, executor);
        }

        private long rejectedTasksCount() {
            return rejectedCount.get();
        }
    }

//    private static class LimitBlockingQueue<T> extends LinkedBlockingQueue<T> {
//
//        private final int highBound;
//
//        private LimitBlockingQueue(int highBound, int capacity) {
//            super(capacity);
//            this.highBound = highBound;
//        }
//
//        @Override
//        public boolean offer(T t) {
//            int size = size();
//            boolean offer = super.offer(t);
//
//            return size <= highBound && offer;
//        }
//    }

    private static class SedaThreadFactory implements ThreadFactory {
        private final AtomicInteger poolNumber = new AtomicInteger(1);
        private final String prefix;

        private SedaThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r);
            thread.setName(prefix + "-" + poolNumber.getAndIncrement());
            return thread;
        }
    }

}
