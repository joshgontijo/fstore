package io.joshworks.fstore.core.util;

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


}
