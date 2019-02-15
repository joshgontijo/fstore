package io.joshworks.fstore.core.util;

import java.util.concurrent.ThreadFactory;

public class Threads {
    private Threads() {

    }

    public static Thread named(String name, Runnable runnable) {
        return thread(name, false, runnable);
    }

    public static Thread thread(String name, boolean daemon, Runnable runnable) {
        Thread thread = new Thread(runnable);
        thread.setName(name);
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


}
