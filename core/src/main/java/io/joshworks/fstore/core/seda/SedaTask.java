package io.joshworks.fstore.core.seda;

import java.util.concurrent.Semaphore;

public class SedaTask implements Runnable {

    private final Runnable delegate;
    final long queuedTime = System.currentTimeMillis();
    long executionTime = 0;
    Semaphore semaphore;

    private SedaTask(Runnable delegate) {
        this.delegate = delegate;
    }

    public static Runnable wrap(Runnable delegate) {
        return new SedaTask(delegate);
    }

    @Override
    public void run() {
        long start = System.currentTimeMillis();
        try {
            delegate.run();
        } finally {
            executionTime = (System.currentTimeMillis()) - start;
            semaphore.release();
        }

    }
}
