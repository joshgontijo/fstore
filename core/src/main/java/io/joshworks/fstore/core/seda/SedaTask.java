package io.joshworks.fstore.core.seda;

public class SedaTask implements Runnable {

    private final Runnable delegate;
    private long executionTime = 0;
    private long started = -1;
    private long created = System.currentTimeMillis();

    private SedaTask(Runnable delegate) {
        this.delegate = delegate;
    }

    public static Runnable wrap(Runnable delegate) {
        return new SedaTask(delegate);
    }

    @Override
    public void run() {
        this.started = System.currentTimeMillis();
        try {
            delegate.run();
        } finally {
            executionTime = (System.currentTimeMillis()) - started;
        }
    }

    public long executionTime() {
        return executionTime;
    }

    public long started() {
        return started;
    }

    public long created() {
        return created;
    }

    public long queueTime() {
        return started < 0 ? System.currentTimeMillis() - created : started - created;
    }
}
