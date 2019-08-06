package io.joshworks.fstore.core.util;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Threads {
    private Threads() {

    }

    public static Thread named(String name, Runnable runnable) {
        return thread(name, false, runnable);
    }

    public static Thread thread(String name, boolean daemon, Runnable runnable) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        return thread;
    }

    public static ThreadFactory namedThreadFactory(String name) {
        return r -> Threads.named(name, r);
    }

    public static ThreadFactory namedThreadFactory(String name, boolean daemon) {
        return r -> Threads.thread(name, daemon, r);
    }

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static <T> T waitFor(Future<T> task) {
        try {
            return task.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(e);
        }
    }

    public static <T> T waitFor(Future<T> task, long timeout, TimeUnit unit) {
        try {
            return task.get(timeout, unit);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException(e);
        }
    }

    public static void awaitTerminationOf(ExecutorService executor, long timeout, TimeUnit timeUnit) {
        try {
            executor.shutdown();
            executor.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void awaitTerminationOf(ExecutorService executor, long checkInterval, TimeUnit timeUnit, Runnable heartbeatTask) {
        try {
            executor.shutdown();
            while (!executor.awaitTermination(checkInterval, timeUnit)) {
                heartbeatTask.run();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static List<Runnable> awaitTerminationOf(ExecutorService executor, long checkInterval, long maxWaitTime, TimeUnit timeUnit, Runnable heartbeatTask) {
        try {
            executor.shutdown();
            long start = System.currentTimeMillis();
            while (!executor.awaitTermination(checkInterval, timeUnit)) {
                long now = System.currentTimeMillis();
                if (now - start > timeUnit.toMillis(maxWaitTime)) {
                    return executor.shutdownNow();
                }
                heartbeatTask.run();
            }
            return Collections.emptyList();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static <T> T futureGet(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}
