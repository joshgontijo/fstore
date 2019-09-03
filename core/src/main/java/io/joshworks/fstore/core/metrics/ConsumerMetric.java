package io.joshworks.fstore.core.metrics;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ConsumerMetric implements Metric {

    private final Consumer<AtomicLong> consumer;
    private final AtomicLong value = new AtomicLong();

    public ConsumerMetric(Consumer<AtomicLong> consumer) {
        this.consumer = consumer;
    }

    @Override
    public long get() {
        consumer.accept(value);
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
