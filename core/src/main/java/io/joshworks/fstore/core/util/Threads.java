package io.joshworks.fstore.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Threads {

    private static AtomicInteger counter = new AtomicInteger();
    public static final Thread.UncaughtExceptionHandler UNCAUGHT_EXCEPTION_HANDLER = (t, e) -> {
        System.err.println("UncaughtException on " + t.getName() + ": " + e.getMessage());
        e.printStackTrace(System.err);
    };

    private Threads() {

    }

    public static Thread spawn(Runnable runnable) {
        return spawn("thread-" + counter.getAndIncrement(), runnable);
    }

    public static Thread spawn(String name, Runnable runnable) {
        var thread = thread(name, false, runnable, UNCAUGHT_EXCEPTION_HANDLER);
        thread.start();
        return thread;
    }

    public static Thread thread(String name, Runnable runnable) {
        return thread(name, false, runnable);
    }

    public static Thread thread(String name, boolean daemon, Runnable runnable) {
        return thread(name, daemon, runnable, UNCAUGHT_EXCEPTION_HANDLER);
    }

    public static Thread thread(String name, boolean daemon, Runnable runnable, Thread.UncaughtExceptionHandler handler) {
        Thread thread = new Thread(runnable, name);
        thread.setDaemon(daemon);
        thread.setUncaughtExceptionHandler(handler);
        return thread;
    }

    public static ThreadFactory namedThreadFactory(String name) {
        return r -> Threads.thread(name, r);
    }

    public static ThreadFactory namePrefixedThreadFactory(String name) {
        return new ThreadFactory() {

            private final AtomicLong factoryCounter = new AtomicLong();

            @Override
            public Thread newThread(Runnable r) {
                return Threads.thread(name + "-" + factoryCounter.getAndIncrement(), r);
            }
        };
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

    public static void waitFor(Thread... threads) {
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                thread.interrupt();
                e.printStackTrace();
            }
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

    public static boolean awaitTermination(ExecutorService executor, long timeout, TimeUnit timeUnit) {
        try {
            executor.shutdown();
            return executor.awaitTermination(timeout, timeUnit);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public static void awaitTermination(ExecutorService executor, long checkInterval, TimeUnit timeUnit, Runnable heartbeatTask) {
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
