package io.joshworks.fstore.core.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class StandardMetric implements Metric {

    public final AtomicLong value = new AtomicLong();

    @Override
    public long get() {
        return value.get();
    }

    @Override
    public void update(long delta) {
        value.addAndGet(delta);
    }

    @Override
    public void set(long value) {
        this.value.set(value);
    }
}
