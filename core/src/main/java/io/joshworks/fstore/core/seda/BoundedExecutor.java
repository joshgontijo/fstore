package io.joshworks.fstore.core.seda;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

class BoundedExecutor {
    private final SedaThreadPoolExecutor executor;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Semaphore semaphore;
    private final boolean blockWhenFull;

    BoundedExecutor(SedaThreadPoolExecutor executor, int bound, boolean blockWhenFull) {
        this.executor = executor;
        this.semaphore = new Semaphore(bound);
        this.blockWhenFull = blockWhenFull;
    }

    void submitTask(Runnable r) {
        if(closed.get()) {
            return;
        }
        boolean acquired = semaphore.tryAcquire();
        if (!blockWhenFull && !acquired) {
            executor.execute(r);
            return;
        }
        if (acquired) {
            execute(r);
            return;
        }
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        execute(r);

    }



    private void execute(Runnable r) {
        if(closed.get()) {
            return;
        }
        try {
            executor.execute(SedaTask.wrap(() -> {
                try {
                    r.run();
                } finally {
                    semaphore.release();
                }
            }));
        } catch (RejectedExecutionException e) {
            semaphore.release();
        }
    }

    public StageStats stats() {
        return new StageStats(executor, closed.get());
    }

    public void shutdown()  {
        closed.set(true);
        executor.shutdown();
    }

    public void awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        executor.awaitTermination(timeout, unit);
    }
}
