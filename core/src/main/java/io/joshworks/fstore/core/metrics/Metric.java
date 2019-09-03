package io.joshworks.fstore.core.metrics;

public interface Metric {

    long get();

    void update(long delta);

    void set(long value);

}
