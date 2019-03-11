package io.joshworks.fstore.testutils;

import io.joshworks.fstore.core.util.Threads;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ThreadUtils {

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2,Threads.namedThreadFactory("watcher", true));

    public static void abortAllIf(Supplier<Boolean> condition, Thread... threads) {
        scheduler.scheduleWithFixedDelay(() -> {
            if(condition.get()) {

            }
        }, 1, 1, TimeUnit.SECONDS);

    }

    public static void watcher(Runnable task) {
        scheduler.scheduleAtFixedRate(task, 2, 2, TimeUnit.SECONDS);

    }

}
